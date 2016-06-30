/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.cloud

import java.io.{File, FileNotFoundException}
import java.net.URI

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileStatus, FileSystem, LocalFileSystem, Path, PathFilter}
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}

/**
 * A cloud suite.
 * Adds automatic loading of a Hadoop configuration file with login credentials and
 * options to enable/disable tests, and a mechanism to conditionally declare tests
 * based on these details
 */
private[cloud] abstract class CloudSuite extends SparkFunSuite with CloudTestKeys
    with LocalSparkContext with BeforeAndAfter with Matchers with TimeOperations
    with ObjectStoreOperations {

  /**
   *  Work under a test directory, so that cleanup works.
   * ...some of the object stores don't implement `delete("/",true)`
   */
  protected val TestDir: Path = {
    val testUniqueForkId: String = System.getProperty(SYSPROP_TEST_UNIQUE_FORK_ID)
    if (testUniqueForkId == null) {
      new Path("/test")
    } else {
      new Path("/" + testUniqueForkId, "test")
    }
  }

  /**
   * The configuration as loaded; may be undefined.
   */
  protected val testConfiguration = loadConfiguration()

  /**
   * Accessor to the configuration, which mist be non-empty.
   */
  protected val conf: Configuration = testConfiguration.get

  /**
   * Map of keys defined on the command line.
   */
  private val testKeyMap = extractTestKeys()

  /**
   * The filesystem.
   */
  private var _filesystem: Option[FileSystem] = None

  /**
   * Accessor for the filesystem.
   * @return the filesystem
   */
  protected def filesystem: FileSystem = _filesystem.get

  /**
   * Probe for the filesystem being defined.
   * @return
   */
  protected def isFilesystemDefined: Boolean = _filesystem.isDefined

  /**
   * URI to the filesystem
   * @return the filesystem URI
   */
  protected def filesystemURI = filesystem.getUri

  private val scaleSizeFactor = conf.getInt(SCALE_TEST_SIZE_FACTOR, SCALE_TEST_SIZE_FACTOR_DEFAULT)
  private val scaleOperationCount = conf.getInt(SCALE_TEST_OPERATION_COUNT,
    SCALE_TEST_OPERATION_COUNT_DEFAULT)

  /**
   * Subclasses may override this for different or configurable test sizes
   * @return the number of entries in parallelized operations.
   */
  protected def testEntryCount = 10 * scaleSizeFactor

  /** this system property is always set in a JVM */
  protected val localTmpDir = new File(System.getProperty("java.io.tmpdir", "/tmp"))
      .getCanonicalFile

  /**
   * Take the test method keys propery, split to a set of keys
   * @return the keys
   */
  private def extractTestKeys(): Set[String] = {
    val property = System.getProperty(SYSPROP_TEST_METHOD_KEYS, "")
    val splits = property.split(',')
    var s: Set[String] = Set()
    for (elem <- splits) {
      val trimmed = elem.trim
      if (!trimmed.isEmpty && trimmed != "null") {
        s = s ++ Set(elem.trim)
      }
    }
    if (s.nonEmpty) {
      logInfo(s"Test keys: $s")
    }
    s
  }

  /**
   * Is a specific test enabled?
   * @param key test key
   * @return true if there were no test keys named, or, if there were, that this key is in the list
   */
  def isTestEnabled(key: String): Boolean = {
    testKeyMap.isEmpty || testKeyMap.contains(key)
  }

  /**
   * A conditional test which is only executed when the suite is enabled, the test key
   * is in the allowed list, and the `extraCondition` predicate holds.
   * @param summary description of the text
   * @param testFun function to evaluate
   * @param detail detailed text for reports
   * @param extraCondition extra predicate which may be evaluated to decide if a test can run.
   */
  protected def ctest(
      key: String,
      summary: String,
      detail: String,
      extraCondition: => Boolean = true)(testFun: => Unit): Unit = {
    val testText = key + ": " + summary
    if (enabled && isTestEnabled(key) && extraCondition) {
      registerTest(testText) {
        logInfo(testText + "\n" + detail + "\n-------------------------------------------")
        testFun
      }
    } else {
      registerIgnoredTest(testText) {
        testFun
      }
    }
  }

  /**
   * A conditional test which is only executed when the suite is enabled
   * @param testText description of the text
   * @param testFun function to evaluate
   */
  protected def ctest(testText: String)(testFun: => Unit): Unit = {
    if (enabled) {
      registerTest(testText) {testFun}
    } else {
      registerIgnoredTest(testText) {testFun}
    }
  }

  /**
   * Update the filesystem; includes a validity check to ensure that the local filesystem
   * is never accidentally picked up.
   * @param fs new filesystem
   */
  protected def setFilesystem(fs: FileSystem): Unit = {
    if (fs.isInstanceOf[LocalFileSystem] || "file" == fs.getScheme) {
      throw new IllegalArgumentException("Test filesystem cannot be local filesystem")
    }
    _filesystem = Some(fs)
  }

  /**
   * Create a filesystem. This adds it as the `filesystem` field.
   * @param fsURI filesystem URI
   * @return the newly create FS.
   */
  protected def createFilesystem(fsURI: URI): FileSystem = {
    val fs = FileSystem.get(fsURI, conf)
    setFilesystem(fs)
    fs
  }

  /**
   * Clean up the filesystem if it is defined.
   */
  protected def cleanFilesystem(): Unit = {
    val target = s"${filesystem.getUri}$TestDir"
    note(s"Cleaning $target")
    if (filesystem.exists(TestDir) && !filesystem.delete(TestDir, true)) {
      logWarning(s"Deleting $target returned false")
    }
  }

  /**
   * Teardown-time cleanup; exceptions are logged and not forwarded
   */
  protected def cleanFilesystemInTeardown(): Unit = {
    try {
      cleanFilesystem()
    } catch {
      case e: Exception =>
        logInfo(s"During cleanup of filesystem: $e")
        logDebug(s"During cleanup of filesystem", e)
    }
  }

  /**
   * Is this test suite enabled?
   * The base class is enabled if the configuration file loaded; subclasses can extend
   * this with extra probes, such as for bindings to an object store.
   *
   * If this predicate is false, then tests defined in `ctest()` will be ignored
   * @return true if the test suite is enabled.
   */
  protected def enabled: Boolean = testConfiguration.isDefined

  /**
   * Load the configuration file from the system property `SYSPROP_CLOUD_TEST_CONFIGURATION_FILE`.
   * @return the configuration
   * @throws FileNotFoundException if a configuration is named but not present.
   */
  protected def loadConfiguration(): Option[Configuration] = {
    val filename = System.getProperty(SYSPROP_CLOUD_TEST_CONFIGURATION_FILE, "")
    logDebug(s"Configuration property = `$filename`")
    if (filename != null && !filename.isEmpty && !CLOUD_TEST_UNSET_STRING.equals(filename)) {
      val f = new File(filename)
      if (f.exists()) {
        logInfo(s"Loading configuration from $f")
        val c = new Configuration(false)
        c.addResource(f.toURI.toURL)
        Some(c)
      } else {
        throw new FileNotFoundException(s"No file '$filename'" +
            s" in property $SYSPROP_CLOUD_TEST_CONFIGURATION_FILE")
      }
    } else {
      None
    }
  }

  /**
   * Get a required option; throw an exception if the key is missing
   * or an empty string.
   * @param key the key to look up
   * @return the trimmed string value.
   */
  protected def requiredOption(key: String): String = {
    val v = conf.getTrimmed(key)
    require(v != null && !v.isEmpty, s"Unset/empty configuration option $key")
    v
  }

  /**
   * Override point for suites: a method which is called
   * in all the `newSparkConf()` methods.
   * This can be used to alter values for the configuration.
   * It is called before the configuration read in from the command line
   * is applied, so that tests can override the values applied in-code.
   * @param sc spark configuration to alter
   */
  protected def addSuiteConfigurationOptions(sc: SparkConf): Unit = {
  }

  /**
   * Create a spark conf, using the current filesystem as the URI for the default FS.
   * All options loaded from the test configuration XML file will be added as hadoop options.
   * @return the configuration
   */
  def newSparkConf(): SparkConf = {
    require(isFilesystemDefined, "Not bonded to a test filesystem")
    newSparkConf(filesystemURI)
  }

  /**
   * Create a spark conf. All options loaded from the test configuration
   * XML file will be added as hadoop options.
   * @param uri the URI of the default filesystem
   * @return the configuration
   */
  def newSparkConf(uri: URI): SparkConf = {
    val sc = new SparkConf(false)
    addSuiteConfigurationOptions(sc)
    conf.asScala.foreach { e =>
      hconf(sc, e.getKey, e.getValue)
    }
    hconf(sc, CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri.toString)
    sc.setMaster("local")
    sc
  }

  /**
   * Creat a new spark configuration under the test filesystem
   * @param path path
   * @return
   */
  def newSparkConf(path: Path): SparkConf = {
    newSparkConf(path.getFileSystem(conf).getUri)
  }

  /**
   * Set a Hadoop configuration option in a spark configuration
   * @param sc spark context
   * @param k configuration key
   * @param v configuration value
   */
  def hconf(sc: SparkConf, k: String, v: String): Unit = {
    sc.set("spark.hadoop." + k, v)
  }



  /**
   * Get the file status of a path
   * @param path path to query
   * @return the status
   * @throws FileNotFoundException if there is no entity there
   */
  def stat(path: Path): FileStatus = {
    filesystem.getFileStatus(path)
  }

  /**
   * Get the filesystem to a path; uses the current configuration
   * @param path path to use
   * @return a (cached) filesystem.
   */
  def getFilesystem(path: Path): FileSystem = {
    FileSystem.get(path.toUri, conf)
  }

}

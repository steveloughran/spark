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
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FSDataInputStream, FileSystem, LocalFileSystem, Path}
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkFunSuite}

/**
 * A cloud suite.
 * Adds automatic loading of a Hadoop configuration file with login credentials and
 * options to enable/disable tests, and a mechanism to conditionally declare tests
 * based on these details
 */
private[spark] class CloudSuite extends SparkFunSuite with CloudTestKeys with LocalSparkContext
    with BeforeAndAfter with Matchers {

  /**
   *  Work under a test directory, so that cleanup works.
   * ...some of the object stores don't implement `delete("/",true)`
   */
  protected val TestDir = new Path("/test")
  val TEST_ENTRY_COUNT = 1000
  protected val conf = loadConfiguration()

  private var _filesystem: Option[FileSystem] = None
  protected def filesystem: Option[FileSystem] = _filesystem
  protected def fsURI = _filesystem.get.getUri

  /**
   * Subclasses may override this for different or configurable test sizes
   * @return the number of entries in parallelized operations.
   */
  protected def testEntryCount: Int = TEST_ENTRY_COUNT

  /** this system property is always set in a JVM */
  protected val localTmpDir = new File(System.getProperty("java.io.tmpdir", "/tmp"))
      .getCanonicalFile

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
    // sanity check: reject anything looking like a local FS
    filesystem.foreach { fs =>
      note(s"Cleaning ${fs.getUri}$TestDir")
      if (!fs.delete(TestDir, true)) {
        logWarning(s"Deleting ${fs.getUri}$TestDir returned false")
      }
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
        logInfo("During cleanup of filesystem: $e", e)
    }
  }



  /**
   * Enabled flag.
   * The base class is enabled if the configuration file loaded; subclasses can extend
   * this with extra probes.
   *
   * If this predicate is false, then tests defined in `ctest()` will be ignored
   * @return true if the test suite is enabled.
   */
  protected def enabled: Boolean = conf != null

  protected def loadConfiguration(): Configuration = {
    val filename = System.getProperty(CLOUD_TEST_CONFIGURATION_FILE, "")
    logDebug(s"Configuration property = `$filename`")
    if (filename != null && !filename.isEmpty && !CLOUD_TEST_UNSET_STRING.equals(filename)) {
      val f = new File(filename)
      if (f.exists()) {
        logInfo(s"Loading configuration from $f")
        val c = new Configuration(false)
        c.addResource(f.toURI.toURL)
        c
      } else {
        throw new FileNotFoundException(s"No file '$filename'" +
            s" in property $CLOUD_TEST_CONFIGURATION_FILE")
      }
    } else {
      null
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
    require(v!=null && !v.isEmpty, s"Unset/empty configuration option $key")
    v
  }

  /**
   * Create a spark conf, using the current filesystem as the URI for the default FS.
   * All options loaded from the test configuration
   * XML file will be added as hadoop options
   * @return the configuration
   */
  def newSparkConf(): SparkConf = {
    require(filesystem.isDefined, "Not bonded to a test filesystem")
    newSparkConf(fsURI)
  }

  /**
   * Create a spark conf. All options loaded from the test configuration
   * XML file will be added as hadoop options
   * @param uri the URI of the default filesystem
   * @return the configuration
   */
  def newSparkConf(uri: URI): SparkConf = {
    val sc = new SparkConf(false)
    def hconf(k: String, v: String) = {
      sc.set("spark.hadoop." + k, v)
    }
    conf.asScala.foreach { e =>
      hconf(e.getKey, e.getValue)
    }
    hconf(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, uri.toString)
    sc
  }

  def newSparkConf(path: Path): SparkConf = {
    newSparkConf(path.getFileSystem(conf).getUri)
  }

  /**
   * A conditional test which is only executed when the suite is enabled
   * @param testText description of the text
   * @param testFun function to evaluate
   */
  protected def ctest(testText: String)(testFun: => Unit): Unit = {
    if (enabled) {
      registerTest(testText) { testFun }
    } else {
      registerIgnoredTest(testText) { testFun }
    }
  }

/*  def readBytesToString(fs: FileSystem, path: Path, length: Int) : String  = {
    val in = fs.open(path)
    try {
      val buf = new Array[Byte](length)
      in.readFully(0, buf)
      toChar(buf)
    } finally {
      in.close
    }
  }*/
}

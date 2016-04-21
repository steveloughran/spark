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
import java.util.Locale

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FSDataInputStream, FileStatus, FileSystem, LocalFileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.rdd.RDD
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
  private val testKeyMap = extractTestKeys()

  /**
   * Subclasses may override this for different or configurable test sizes
   * @return the number of entries in parallelized operations.
   */
  protected def testEntryCount: Int = TEST_ENTRY_COUNT

  /** this system property is always set in a JVM */
  protected val localTmpDir = new File(System.getProperty("java.io.tmpdir", "/tmp"))
      .getCanonicalFile

  /**
   * Take the test method keys propery, split to a set of keys
   * @return the keys
   */
  private def extractTestKeys(): Set[String] = {
    val property = System.getProperty(TEST_METHOD_KEYS, "")
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
   * Is a specific test enabled
   * @param key test key
   * @return true if there were no test keys name, or, if there were, is this key in the list
   */
  def isTestEnabled(key: String): Boolean = {
    testKeyMap.isEmpty || testKeyMap.contains(key)
  }

  /**
   * A conditional test which is only executed when the suite is enabled
   * @param summary description of the text
   * @param testFun function to evaluate
   */
  protected def ctest(key: String, summary: String, detail: String)(testFun: => Unit): Unit = {
    val testText = key + ": " + summary
    if (enabled && isTestEnabled(key)) {
      registerTest(testText) {testFun}
    } else {
      registerIgnoredTest(summary) {testFun}
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
   * Measure the duration of an operation, log it with the text
   * @param operation operation description
   * @param testFun function to execute
   * @return the result
   */
  def duration[T](operation: String)(testFun: => T): T = {
    val start = nanos
    try {
      testFun
    } finally {
      val end = nanos()
      val d = end - start
      logInfo(s"Duration of $operation = ${toHuman(d)} ns")
    }
  }

  /**
   * Measure the duration of an operation, log it
   * @param testFun function to execute
   * @return the result and the operation duration in nanos
   */
  def duration2[T](testFun: => T): (T, Long, Long) = {
    val start = nanos()
    try {
      var r = testFun
      val end = nanos()
      val d = end - start
      (r, start, d)
    } catch {
      case ex: Exception =>
        val end = nanos()
        val d = end - start
        logError("After ${toHuman(d)} ns: $ex", ex)
        throw ex
    }
  }

  def nanos(): Long = {
    System.nanoTime()
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   *
   * There's a bit of convoluted-ness here, as this supports writing to any Hadoop FS,
   * rather than the default one in the configuration ... this is addressed by creating a
   * new configuration
   */
  def saveAsTextFile[T](rdd: RDD[T], path: Path, conf: Configuration)
  : Unit = {
    rdd.withScope {
      val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
      val textClassTag = implicitly[ClassTag[Text]]
      val r = rdd.mapPartitions { iter =>
        val text = new Text()
        iter.map { x =>
          text.set(x.toString)
          (NullWritable.get(), text)
        }
      }
      val pathFS = FileSystem.get(path.toUri, conf)
      val confWithTargetFS = new Configuration(conf)
      confWithTargetFS.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY,
        pathFS.getUri.toString)
      val pairOps = RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      pairOps.saveAsNewAPIHadoopFile(path.toUri.toString,
        pairOps.keyClass, pairOps.valueClass,
        classOf[org.apache.hadoop.mapreduce.lib.output.TextOutputFormat[NullWritable, Text]],
        confWithTargetFS)
    }
  }

  def stat(path: Path): FileStatus = {
    filesystem.get.getFileStatus(path)
  }

  def getFS(path: Path): FileSystem = {
    FileSystem.get(path.toUri, conf)
  }

  def toHuman(ns: Long): String = {
    String.format(Locale.ENGLISH, "%,010d", ns.asInstanceOf[Object])
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

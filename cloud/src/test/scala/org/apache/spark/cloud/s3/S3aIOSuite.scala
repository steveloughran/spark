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

package org.apache.spark.cloud.s3

import java.io.FileNotFoundException
import java.net.URI

import scala.collection.mutable

import org.apache.hadoop.fs.s3a.Constants
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FSDataInputStream, Path}

import org.apache.spark.SparkContext
import org.apache.spark.cloud.CloudSuite

private[spark] class S3aIOSuite extends CloudSuite {


  val sceneList = new Path(S3_CSV_PATH)

  /** number of lines, from `gunzip` + `wc -l` on day tested. This grows over time*/
  val ExpectedSceneListLines = 447919

  override def enabled: Boolean = super.enabled && conf.getBoolean(AWS_TESTS_ENABLED, false)

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      val id = requiredOption(AWS_ACCOUNT_ID)
      val secret = requiredOption(AWS_ACCOUNT_SECRET)
      conf.set("fs.s3n.awsAccessKeyId", id)
      conf.set("fs.s3n.awsSecretAccessKey", secret)
      conf.set(Constants.BUFFER_DIR, localTmpDir.getAbsolutePath)
      val s3aURI = new URI(requiredOption(S3_TEST_URI))
      logInfo(s"Executing S3 tests against $s3aURI")
      createFilesystem(s3aURI)
    }
  }

  after {
    cleanFilesystemInTeardown()
  }

  ctest("mkdirs", "Create, delete directory", "") {
    val fs = filesystem.get
    val path = TestDir
    fs.mkdirs(path)
    val st = stat(path)
    logInfo(s"Created filesystem entry $path: $st")
    fs.delete(path, true)
    intercept[FileNotFoundException] {
      val st2 = stat(path)
      logError(s"Got status $st2")
    }
  }

  ctest("FileOutput", "Generate then Read data -File Output Committer", "") {
    sc = new SparkContext("local", "test", newSparkConf())
    val conf = sc.hadoopConfiguration
    assert(fsURI.toString === conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
    val entryCount = testEntryCount
    val numbers = sc.parallelize(1 to entryCount)
    val example1 = new Path(TestDir, "example1")
    numbers.saveAsTextFile(example1.toString)
    val st = stat(example1)
    assert(st.isDirectory, s"Not a dir: $st")
    val fs = filesystem.get
    val children = fs.listStatus(example1)
    assert(children.nonEmpty, s"No children under $example1")
    children.foreach { child =>
      logInfo(s"$child")
      assert(child.getLen > 0 || child.getPath.getName === "_SUCCESS",
        s"empty output $child")
    }
    val parts = children.flatMap { child =>
      if (child.getLen > 0) Seq(child) else Nil
    }
    assert(parts.length === 1)
    val parts0 = parts(0)
    // now read it in
    val input = sc.textFile(parts0.getPath.toString)
    val results = input.collect()
    assert(entryCount === results.length, s"size of results read in from $parts0")
  }

  ctest("New Hadoop API", "New Hadoop API", "") {
    sc = new SparkContext("local", "test", newSparkConf())
    val conf = sc.hadoopConfiguration
    val entryCount = testEntryCount
    val numbers = sc.parallelize(1 to entryCount)
    val example1 = new Path(TestDir, "example1")
    saveAsTextFile(numbers, example1, conf)
  }

  ctest("CSVgz", "Read compressed CSV", "") {
    val source = sceneList
    sc = new SparkContext("local", "test", newSparkConf(source))
    val sceneInfo = getFS(source).getFileStatus(source)
    logInfo(s"Compressed size = ${sceneInfo.getLen}")
    val input = sc.textFile(source.toString)
    val count = input.count()
    logInfo(s" size of $source = $count rows")
    assert(ExpectedSceneListLines <= count,
      s"Number of rows in $source [$count] less than expected value $ExpectedSceneListLines")
  }

  ctest("CSVdiffFS", "Read compressed CSV differentFS", "") {
    sc = new SparkContext("local", "test", newSparkConf())
    val source = sceneList
    val input = sc.textFile(source.toString)
    val count = input.count()
    logInfo(s" size of $source = $count rows")
    assert(ExpectedSceneListLines === count, s"Number of rows in $source")
  }

  /**
   * Assess cost of seek and read operations.
   * When moving the cursor in an input stream, an HTTP connection may be closed and
   * then re-opened. This can be very expensive; tactics like streaming forwards instead
   * of seeking, and/or postponing movement until the following read ("lazy seek") try
   * to address this. Logging these operation times helps track performance.
   * This test also tries to catch out a regression, where a `close()` operation
   * is implemented through reading through the entire input stream. This is exhibited
   * in the time to `close()` while at offset 0 being `O(len(file))`.
   *
   * Note also the cost of `readFully()`; this method call is common inside libraries
   * like Orc and Parquet.
   */
  ctest("SeekClose", "Cost of seek and close",
    """Assess cost of seek and read operations.
      | When moving the cursor in an input stream, an HTTP connection may be closed and
      | then re-opened. This can be very expensive; tactics like streaming forwards instead
      | of seeking, and/or postponing movement until the following read ('lazy seek') try
      | to address this. Logging these operation times helps track performance.
      | This test also tries to catch out a regression, where a `close()` operation
      | is implemented through reading through the entire input stream. This is exhibited
      | in the time to `close()` while at offset 0 being `O(len(file))`.
      | Note also the cost of `readFully()`; this method call is common inside libraries
      | like Orc and Parquet.""".stripMargin) {
    val source = sceneList
    val fs = getFS(source)
    val st = duration("stat") {
      fs.getFileStatus(source)
    }
    val out = duration("open") {
      fs.open(source)
    }
    duration("read() [0]") {
      assert(-1 !== out.read())
    }
    duration("seek(256) [1]") {
      out.seek(256)
    }
    duration("seek(256) [256]") {
      out.seek(256)
    }
    duration("seek(EOF-2)") {
      out.seek(st.getLen - 2)
    }
    duration("read() [EOF-2]") {
      assert(-1 !== out.read())
    }
    duration("readFully([1, byte[1]]) [EOF-1]") {
      val bytes = new Array[Byte](1)
      assert(-1 !== out.readFully(1L, bytes))
    }
    logInfo(s"current position ${out.getPos}")
    duration("readFully[1, byte[1024]] [EOF-1]") {
      val bytes = new Array[Byte](1024)
      assert(-1 !== out.readFully(1L, bytes))
    }
    duration("seek(256) [EOF-1]") {
      out.seek(256)
    }
    duration("read() [offset=256]") {
      assert(-1 !== out.read())
    }
    duration("close()") {
      out.close
    }
  }

  ctest("ReadBytesReturned", "Read Bytes",
    """Read in blocks and assess their size and duration.
      | This is to identify buffering quirks
    """.stripMargin) {
    val source = sceneList
    val fs = getFS(source)
    val blockSize = 8192
    val buffer = new Array[Byte](blockSize)
    val returnSizes: mutable.Map[Int, (Int, Long)] = mutable.Map()
    val stat = fs.getFileStatus(source)
    val blocks = (stat.getLen/blockSize).toInt
    val instream: FSDataInputStream = fs.open(source)
    var readOperations = 0
    var totalReadTime = 0L
    for (i <- 1 to blocks) {
      var offset = 0
      while(offset < blockSize) {
        readOperations += 1
        val (bytesRead, time) = duration2 {
          instream.read(buffer, offset, blockSize - offset)
        }
        assert(bytesRead> 0,  s"In block $i read from offset $offset returned $bytesRead")
        offset += bytesRead
        totalReadTime += time
        val current = returnSizes.getOrElse(bytesRead, (0, 0L))
        returnSizes(bytesRead) = (1 + current._1, time + current._2)
//        Thread.sleep(1)
      }
    }
    // completion
    logInfo(
      s"""$blocks blocks of size $blockSize;
         | total #of read operations $readOperations;
         | total read time=$totalReadTime;
         | ${totalReadTime/(blocks * blockSize)} ns/byte""".stripMargin)
    returnSizes.toSeq.sortBy(_._1).foreach { v  =>
      val k = v._1
      val c = v._2._1
      val d = v._2._2
      logInfo(s"[$k] count = $c average duration = ${d/c}")
    }
//    sc.parallelize(returnSizes.toSeq)
  }

}

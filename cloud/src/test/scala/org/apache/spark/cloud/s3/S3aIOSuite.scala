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
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FSDataInputStream, FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.cloud.CloudSuite
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

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
    logInfo(s"Filesystem statistics $fs")
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
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics ${getFS(source)}")
  }

  def validateCSV(ctx: SparkContext, source: Path): Unit = {
    val input = ctx.textFile(source.toString)
    val (count, started, time) = duration2 {
      input.count()
    }
    logInfo(s" size of $source = $count rows read in $time nS")
    assert(ExpectedSceneListLines <= count,
      s"Number of rows in $source [$count] less than expected value $ExpectedSceneListLines")
  }

  ctest("CSVdiffFS", "Read compressed CSV differentFS", "") {
    sc = new SparkContext("local", "test", newSparkConf())
    val source = sceneList
    val input = sc.textFile(source.toString)
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics ${getFS(source)}")
  }

  ctest("SeekReadFully", "Cost of seek and ReadFully",
    """Assess cost of seek and read operations.
      | When moving the cursor in an input stream, an HTTP connection may be closed and
      | then re-opened. This can be very expensive; tactics like streaming forwards instead
      | of seeking, and/or postponing movement until the following read ('lazy seek') try
      | to address this. Logging these operation times helps track performance.
      | This test also tries to catch out a regression, where a `close()` operation
      | is implemented through reading through the entire input stream. This is exhibited
      | in the time to `close()` while at offset 0 being `O(len(file))`.
      |
      | Note also the cost of `readFully()`; this method call is common inside libraries
      | like Orc and Parquet.""".stripMargin) {
    val source = sceneList
    val fs = getFS(source)
    FileSystem.clearStatistics
    val stats = FileSystem.getStatistics("s3a", fs.getClass)
    stats.reset()
    val st = duration("stat") {
      fs.getFileStatus(source)
    }
    val in = duration("open") {
      fs.open(source)
    }
    def time[T](operation: String)(testFun: => T): T = {
      logInfo(s"")
      var r = duration(operation + s" [pos = ${in.getPos}]")(testFun)
      logInfo(s"  ${in.getWrappedStream}")
      r
    }

    time("read()") {
      assert(-1 !== in.read())
    }
    time("seek(256)") {
      in.seek(256)
    }
    time("seek(256)") {
      in.seek(256)
    }
    time("seek(EOF-2)") {
      in.seek(st.getLen - 2)
    }
    time("read()") {
      assert(-1 !== in.read())
    }

    def readFully(offset: Long, len: Int) = {
      time(s"readFully($offset, byte[$len])") {
        val bytes = new Array[Byte](len)
        assert(-1 !== in.readFully(offset, bytes))
      }
    }
    readFully(1L, 1)
    readFully(1L, 256)
    readFully(260L, 256)
    readFully(1024L, 256)
    readFully(1536L, 256)
    readFully(8192L, 1024)
    readFully(8192L + 1024 + 512, 1024)

    time("seek(getPos)") {
      in.seek(in.getPos())
    }
    time("read()") {
      assert(-1 !== in.read())
    }
    duration("close()") {
      in.close
    }
    logInfo(s"Statistics $stats")
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
    val results = new mutable.MutableList[ReadSample]()
    for (i <- 1 to blocks) {
      var offset = 0
      while(offset < blockSize) {
        readOperations += 1
        val requested = blockSize - offset
        val pos = instream.getPos
        val (bytesRead, started, time) = duration2 {
          instream.read(buffer, offset, requested)
        }
        assert(bytesRead> 0,  s"In block $i read from offset $offset returned $bytesRead")
        offset += bytesRead
        totalReadTime += time
        val current = returnSizes.getOrElse(bytesRead, (0, 0L))
        returnSizes(bytesRead) = (1 + current._1, time + current._2)
        val sample = new ReadSample(started, time, blockSize, requested, bytesRead, pos)
        results += sample
      }
    }
        // completion
    logInfo(
      s"""$blocks blocks of size $blockSize;
         | total #of read operations $readOperations;
         | total read time=${toHuman(totalReadTime)};
         | ${totalReadTime/(blocks * blockSize)} ns/byte""".stripMargin)


    logInfo("Read sizes")
    returnSizes.toSeq.sortBy(_._1).foreach { v  =>
      val returnedBytes = v._1
      val count = v._2._1
      val totalDuration = v._2._2
      logInfo(s"[$returnedBytes] count = $count" +
          s" average duration = ${totalDuration/count}" +
          s" nS/byte = ${totalDuration/(count * returnedBytes)}")
    }

    // spark analysis
    sc = new SparkContext("local", "test", newSparkConf())

    val resultsRDD = sc.parallelize(results)
    val blockFrequency = resultsRDD.map(s => (s.blockSize, 1))
        .reduceByKey((v1, v2) => v1 + v2)
        .sortBy(_._2, false)
    logInfo(s"Most frequent sizes:\n")
    blockFrequency.toLocalIterator.foreach{ t =>
      logInfo(s"[${t._1}]: ${t._2}\n")
    }
    val resultsVector = resultsRDD.map( _.toVector)
    val stats = Statistics.colStats(resultsVector)
    logInfo(s"Bytes Read ${summary(stats, 4)}")
    logInfo(s"Difference between requested and actual ${summary(stats, 7)}")
    logInfo(s"Per byte read time/nS ${summary(stats, 6)}")
    logInfo(s"Filesystem statistics ${getFS(source)}")
  }

  def summary(stats: MultivariateStatisticalSummary, col: Int): String = {
    val b = new StringBuilder(256)
    b.append(s"min=${stats.min(col)}; ")
    b.append(s"max=${stats.max(col)}; ")
    b.append(s"mean=${stats.mean(col)}; ")
    b.toString()
  }
}

private class ReadSample(
    val started: Long,
    val duration: Long,
    val blockSize: Int,
    val bytesRequested: Int,
    val bytesRead: Int,
    val pos: Long) extends Serializable {

  def perByte: Long = { if (duration > 0)  bytesRead / duration else -1L }

  def delta: Int = { bytesRequested - bytesRead }

  override def toString = s"ReadSample(started=$started, duration=$duration," +
      s" blockSize=$blockSize, bytesRequested=$bytesRequested, bytesRead=$bytesRead)" +
      s" pos=$pos"

  def toVector: org.apache.spark.mllib.linalg.Vector = {
    val a = new Array[Double](8)
    a(0) = started.toDouble
    a(1) = duration.toDouble
    a(2) = blockSize.toDouble
    a(3) = bytesRequested.toDouble
    a(4) = bytesRead.toDouble
    a(5) = pos.toDouble
    a(6) = perByte.toDouble
    a(7) = delta.toDouble
    Vectors.dense(a)
  }

}

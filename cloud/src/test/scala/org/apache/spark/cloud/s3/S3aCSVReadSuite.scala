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

import scala.collection.mutable

import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.cloud.CloudSuite
import org.apache.spark.cloud.common.ReadSample
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

/**
 * A suite of tests reading in the S3A CSV file.
 */
private[cloud] class S3aCSVReadSuite extends CloudSuite with S3aTestSetup {

  /**
   * Minimum number of lines, from `gunzip` + `wc -l` on day of first teste.
   * This grows over time.
   */
  val ExpectedSceneListLines = 447919

  override def enabled: Boolean = super.enabled && hasCSVTestFile

  init()

  def init(): Unit = {
    // propagate S3 credentials
    if (enabled) {
      initFS()
    }
  }

  after {
    cleanFilesystemInTeardown()
  }

  ctest("CSVgz",
    "Read compressed CSV",
    "Read compressed CSV files through the spark context") {
    val source = CSV_TESTFILE.get
    sc = new SparkContext("local", "CSVgz", newSparkConf(source))
    val sceneInfo = getFilesystem(source).getFileStatus(source)
    logInfo(s"Compressed size = ${sceneInfo.getLen}")
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics ${getFilesystem(source)}")
  }

  /**
   * Validate the CSV by loading it, counting the number of lines and verifying that it
   * is at least as big as that expected. This minimum size test verifies that the source file
   * was read, even when that file is growing from day to day.
   * @param ctx context
   * @param source source object
   * @param lines the minimum number of lines whcih the source must have.
   * @return the actual count of lines read
   */
  def validateCSV(ctx: SparkContext, source: Path, lines: Long = ExpectedSceneListLines): Long = {
    val input = ctx.textFile(source.toString)
    val (count, started, time) = duration2 {
      input.count()
    }
    logInfo(s" size of $source = $count rows read in $time nS")
    assert(lines <= count,
      s"Number of rows in $source [$count] less than expected value $lines")
    count
  }

  ctest("CSVdiffFS",
    "Read compressed CSV differentFS",
    """Use a compressed CSV from the non-default FS.
      | This verifies that the URIs are directing to the correct FS""".stripMargin) {
    sc = new SparkContext("local", "test", newSparkConf())
    val source = CSV_TESTFILE.get
    validateCSV(sc, source)
    logInfo(s"Filesystem statistics ${getFilesystem(source)}")
  }

  ctest("SeekReadFully",
    "Cost of seek and ReadFully",
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
    val source = CSV_TESTFILE.get
    val fs = getFilesystem(source)
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

    def readFully(offset: Long, len: Int): Unit = {
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

  ctest("ReadBytesReturned",
    "Read Bytes",
    """Read in blocks and assess their size and duration.
      | This is to identify buffering quirks.
    """.stripMargin) {
    val source = CSV_TESTFILE.get
    val blockSize = 8192
    val buffer = new Array[Byte](blockSize)
    val returnSizes: mutable.Map[Int, (Int, Long)] = mutable.Map()
    val stat = getFilesystem(source).getFileStatus(source)
    val blocks = (stat.getLen / blockSize).toInt
    val instream: FSDataInputStream = getFilesystem(source).open(source)
    var readOperations = 0
    var totalReadTime = 0L
    val results = new mutable.MutableList[ReadSample]()
    for (i <- 1 to blocks) {
      var offset = 0
      while (offset < blockSize) {
        readOperations += 1
        val requested = blockSize - offset
        val pos = instream.getPos
        val (bytesRead, started, time) = duration2 {
          instream.read(buffer, offset, requested)
        }
        assert(bytesRead > 0, s"In block $i read from offset $offset returned $bytesRead")
        offset += bytesRead
        totalReadTime += time
        val current = returnSizes.getOrElse(bytesRead, (0, 0L))
        returnSizes(bytesRead) = (1 + current._1, time + current._2)
        val sample = new ReadSample(started, time, blockSize, requested, bytesRead, pos)
        results += sample
      }
    }
    logInfo(
      s"""$blocks blocks of size $blockSize;
         | total #of read operations $readOperations;
         | total read time=${toHuman(totalReadTime)};
         | ${totalReadTime / (blocks * blockSize)} ns/byte""".stripMargin)


    logInfo("Read sizes")
    returnSizes.toSeq.sortBy(_._1).foreach { v =>
      val returnedBytes = v._1
      val count = v._2._1
      val totalDuration = v._2._2
      logInfo(s"[$returnedBytes] count = $count" +
          s" average duration = ${totalDuration / count}" +
          s" nS/byte = ${totalDuration / (count * returnedBytes)}")
    }

    // spark analysis
    sc = new SparkContext("local", "test", newSparkConf())

    val resultsRDD = sc.parallelize(results)
    val blockFrequency = resultsRDD.map(s => (s.blockSize, 1))
        .reduceByKey((v1, v2) => v1 + v2)
        .sortBy(_._2, false)
    logInfo(s"Most frequent sizes:\n")
    blockFrequency.toLocalIterator.foreach { t =>
      logInfo(s"[${t._1}]: ${t._2}\n")
    }
    val resultsVector = resultsRDD.map(_.toVector)
    val stats = Statistics.colStats(resultsVector)
    logInfo(s"Bytes Read ${summary(stats, 4)}")
    logInfo(s"Difference between requested and actual ${summary(stats, 7)}")
    logInfo(s"Per byte read time/nS ${summary(stats, 6)}")
    logInfo(s"Filesystem statistics ${getFilesystem(source)}")
  }

  def summary(stats: MultivariateStatisticalSummary, col: Int): String = {
    val b = new StringBuilder(256)
    b.append(s"min=${stats.min(col)}; ")
    b.append(s"max=${stats.max(col)}; ")
    b.append(s"mean=${stats.mean(col)}; ")
    b.toString()
  }
}

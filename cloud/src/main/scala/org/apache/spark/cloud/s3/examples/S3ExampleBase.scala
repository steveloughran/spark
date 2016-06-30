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

package org.apache.spark.cloud.s3.examples

import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem, Path}
import org.apache.hadoop.io.{NullWritable, Text}

import org.apache.spark.cloud.TimeOperations
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

/**
 * Base Class for examples working with S3.
 */
private[cloud] trait S3ExampleBase extends TimeOperations {
  /**
   * Default source of a public multi-MB CSV file.
   */
  val S3A_CSV_PATH_DEFAULT = "s3a://landsat-pds/scene_list.gz"

  val EXIT_USAGE = -2
  val EXIT_ERROR = -1

  /**
   * Execute an operation, using its return value as the System exit code.
   * Exceptions are caught, logged and an exit code of -1 generated.
   *
   * @param operation operation to execute
   * @param args list of arguments from the command line
   */
  protected def execute(operation: (SparkConf, Array[String]) => Int, args: Array[String]): Unit = {
    var exitCode = 0
    try {
      val conf = new SparkConf()
      exitCode = operation(conf, args)
    } catch {
      case e: Exception =>
        logError(s"Failed to execute operation: $e", e)
        // in case this is caused by classpath problems, dump it out
        logInfo(s"Classpath =\n${System.getProperty("java.class.path")}")
        exitCode = EXIT_ERROR
    }
    logInfo(s"Exit code = $exitCode")
    exit(exitCode)
  }

  /**
   * Set a hadoop option in a spark configuration
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */
  def hconf(sparkConf: SparkConf, k: String, v: String): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v)
  }

  /**
   * Set a long hadoop option in a spark configuration
   * @param sparkConf configuration to update
   * @param k key
   * @param v new value
   */
  def hconf(sparkConf: SparkConf, k: String, v: Long): Unit = {
    sparkConf.set(s"spark.hadoop.$k", v.toString)
  }

  /**
   * Exit the system.
   * This may be overriden for tests: code must not assume that it never returns.
   * @param exitCode exit code to exit with.
   */
  def exit(exitCode: Int): Unit = {
    System.exit(exitCode)
  }

  protected def intArg(args: Array[String], index: Int, defVal: Int): Int = {
    if (args.length > index) args(index).toInt else defVal
  }
  protected def arg(args: Array[String], index: Int, defVal: String): String = {
    if (args.length > index) args(index) else defVal
  }

  protected def arg(args: Array[String], index: Int): Option[String] = {
    if (args.length > index) Some(args(index)) else None
  }

  /**
   * Save this RDD as a text file, using string representations of elements.
   *
   * There's a bit of convoluted-ness here, as this supports writing to any Hadoop FS,
   * rather than the default one in the configuration ... this is addressed by creating a
   * new configuration
   */
  def saveAsTextFile[T](rdd: RDD[T], path: Path, conf: Configuration): Unit = {
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
}

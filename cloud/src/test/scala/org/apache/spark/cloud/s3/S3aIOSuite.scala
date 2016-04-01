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

import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileStatus, Path}
import org.apache.hadoop.fs.s3a.Constants

import org.apache.spark.SparkContext
import org.apache.spark.cloud.CloudSuite

private[spark] class S3aIOSuite extends CloudSuite {

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

  ctest("Create, delete directory") {
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

  def stat(path: Path): FileStatus = {
    filesystem.get.getFileStatus(path)
  }

  ctest("Generate then Read data -File Output Committer") {
    sc = new SparkContext("local", "test", newSparkConf())
    val conf = sc.hadoopConfiguration
    assert(fsURI.toString === conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY))
    val numbers = sc.parallelize(1 to 1000)
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
    input.collect()
  }
}

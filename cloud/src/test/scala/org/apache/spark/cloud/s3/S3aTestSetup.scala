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

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.s3a.Constants

import org.apache.spark.cloud.CloudSuite

/**
 * Trait for S3A tests
 */
private[cloud] trait S3aTestSetup extends CloudSuite {

  def initFS(): FileSystem = {
    val id = requiredOption(AWS_ACCOUNT_ID)
    val secret = requiredOption(AWS_ACCOUNT_SECRET)
    conf.set("fs.s3n.awsAccessKeyId", id)
    conf.set("fs.s3n.awsSecretAccessKey", secret)
    conf.set(Constants.BUFFER_DIR, localTmpDir.getAbsolutePath)
    // a block size of 1MB
    conf.set(S3AConstants.FS_S3A_BLOCK_SIZE, (1024 * 1024).toString)
    val s3aURI = new URI(requiredOption(S3A_TEST_URI))
    logDebug(s"Executing S3 tests against $s3aURI")
    createFilesystem(s3aURI)
  }

  val CSV_TESTFILE: Option[Path] = {
    val pathname = conf.get(S3A_CSVFILE_PATH, S3A_CSV_PATH_DEFAULT)
    if (!pathname.isEmpty) Some(new Path(pathname)) else None
  }

  protected def hasCSVTestFile = CSV_TESTFILE.isDefined

}

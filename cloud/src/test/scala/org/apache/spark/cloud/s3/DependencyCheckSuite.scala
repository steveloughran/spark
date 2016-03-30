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

import com.amazonaws.services.s3.S3ClientOptions
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.fs.s3native.NativeS3FileSystem
import org.jets3t.service.S3ServiceException

import org.apache.spark.SparkFunSuite

/**
 * Force load in hadoop s3n/s3a classes and some dependencies.
 * Dependency problems should be picked up at compile time; runtime may
 * identify problems with any other transitive library
 */
private[spark] class DependencyCheckSuite extends SparkFunSuite {

  test("Create Jets3t class") {
    new S3ServiceException("jets3t")
  }

  test("Create Amazon s3 class") {
    new S3ClientOptions
  }

  test("Create s3n class") {
    new NativeS3FileSystem
  }

  test("Create s3a class") {
    new S3AFileSystem
  }

}

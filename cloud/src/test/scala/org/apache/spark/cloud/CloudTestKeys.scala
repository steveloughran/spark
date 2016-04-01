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

import org.apache.hadoop.fs.s3a.Constants

/**
 * The various test keys for the cloud tests.
 *
 * Different infrastructure tests may enabled/disabled.
 *
 * Timeouts and scale options are tuneable: this is important for remote test runs.
 *
 * All properties are set in the Java properties file referenced in the System property
 * `cloud.test.configuration.file`; this must be passed down by the test runner. If not set,
 * tests against live cloud infrastructures will be skipped.
 *
 * Important: Test configuration files containing cloud login credentials SHOULD NOT be saved to
 * any private SCM repository, and MUST NOT be saved into any public repository.
 * The best practise for this is: do not ever keep the keys in a directory which is part of
 * an SCM-managed source tree. If absolutely necessary, use a `.gitignore` or or equivalent
 * to ignore the files.
 *
 * It is possible to use XML XInclude references within a configuration file.
 * This allows for the credentials to be retained in a private location, while the rest of the
 * configuration can be managed under SCM:
 *
 *```
 *<configuration>
 *  <include xmlns="http://www.w3.org/2001/XInclude" href="/shared/security/auth-keys.xml"/>
 *</configuration>
 * ```
 */
private[spark] trait CloudTestKeys {

  val CLOUD_TEST_CONFIGURATION_FILE = "cloud.test.configuration.file"

  /**
   * Maven doesn't pass down empty properties as strings; it converts them to the string "null".
   * Here a special string is used to handle that scenario to make it clearer what's happening.
   */
  val CLOUD_TEST_UNSET_STRING = "(unset)"

  /**
   * Are AWS tests enabled? If set, the user
   * must have AWS login credentials, either via the environment
   * or from properties in the test properties file.
   */
  val AWS_TESTS_ENABLED = "aws.tests.enabled"

  /**
   * Amazon Web Services Account
   */
  val AWS_ACCOUNT_ID = Constants.ACCESS_KEY

  /**
   * Amazon Web Services account secret.
   * This is the value which must be reset if it is ever leaked. The tests *must not* log
   * this to any output.
   */
  val AWS_ACCOUNT_SECRET = Constants.SECRET_KEY

  /**
   * A test bucket for S3. All data in this bucket will be deleted during test suite teardowns;
   */
  val S3_TEST_URI = "s3a.test.uri"

  /**
   * Source of a public multi-MB CSV file
   */
  val S3_CSV_PATH = "s3a://landsat-pds.s3.amazonaws.com/scene_list.gz"

}

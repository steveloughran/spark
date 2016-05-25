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

  /**
   * A system property which will be set on parallel test runs
   */
  val SYSPROP_TEST_UNIQUE_FORK_ID = "test.unique.fork.id"

  /**
   * Optional system property for test keys; if set, built into a map of test to run.
   */
  val SYSPROP_TEST_METHOD_KEYS = "test.method.keys"

  /**
   * Name of the configuration file to load for test configuration.
   */
  val SYSPROP_CLOUD_TEST_CONFIGURATION_FILE = "cloud.test.configuration.file"

  /**
   * Maven doesn't pass down empty properties as strings; it converts them to the string "null".
   * Here a special string is used to handle that scenario to make it clearer what's happening.
   */
  val CLOUD_TEST_UNSET_STRING = "(unset)"

  /**
   * Prefix for scale tests
   */
  val SCALE_TEST = "scale.test."

  val SCALE_TEST_OPERATION_COUNT = SCALE_TEST + "operation.count"
  val SCALE_TEST_OPERATION_COUNT_DEFAULT = 10

  /**
   * Scale factor as a percentage of "default" load. Test runners may wish to scale
   * this down as well as up.
   */
  val SCALE_TEST_SIZE_FACTOR = SCALE_TEST + "size.factor"
  val SCALE_TEST_SIZE_FACTOR_DEFAULT = 100

  /**
   * Key defining the Amazon Web Services Account.
   */
  val AWS_ACCOUNT_ID = Constants.ACCESS_KEY

  /**
   * Key defining the Amazon Web Services account secret.
   * This is the value which must be reset if it is ever leaked. The tests *must not* log
   * this to any output.
   */
  val AWS_ACCOUNT_SECRET = Constants.SECRET_KEY

  /**
   * Key defining the Are AWS tests enabled? If set, the user
   * must have AWS login credentials, defined via the environment
   * or in the XML test configuration file.
   */
  val S3A_TESTS_ENABLED = "s3a.tests.enabled"

  /**
   * A test bucket for S3.
   * Data in this bucket under the test directory will be deleted during test suite teardowns;
   */
  val S3A_TEST_URI = "s3a.test.uri"

  /**
   * Key referring to the csvfile. If unset, use `DEFAULT_S3A_CSV_PATH`. If empty, tests
   * depending upon the CSV file will be skipped.
   */
  val S3A_CSVFILE_PATH = "s3a.test.csvfile.path"

  /**
   * Default source of a public multi-MB CSV file.
   */
  val S3A_CSV_PATH_DEFAULT = "s3a://landsat-pds/scene_list.gz"

  /**
   * Key defining the Are AWS tests enabled? If set, the user
   * must have AWS login credentials, defined via the environment
   * or in the XML test configuration file.
   */
  val AZURE_TESTS_ENABLED = "azure.tests.enabled"

  /**
   * A test bucket for Azure.
   * Data in this bucket under the test directory will be deleted during test suite teardowns;
   */
  val AZURE_TEST_URI = "azure.test.uri"
}

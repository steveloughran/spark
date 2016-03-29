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

import org.apache.hadoop.conf.Configuration
import org.scalatest.{BeforeAndAfter, Matchers}

import org.apache.spark.{LocalSparkContext, SparkFunSuite}

class CloudSuite extends SparkFunSuite with CloudTestKeys with LocalSparkContext
    with BeforeAndAfter with Matchers {

  val conf = loadConfiguration()

  def enabled: Boolean = conf != null

  def loadConfiguration(): Configuration = {
    val filename = System.getProperty(CLOUD_TEST_CONFIGURATION_FILE, "")
    logDebug(s"Configuration property = `$filename`")
    if (filename != null && !filename.isEmpty && !CLOUD_TEST_UNSET_STRING.equals(filename)) {
      val f = new File(filename)
      if (f.exists()) {
        logDebug(s"Loading configuration from $f")
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
   * A conditional test which is only executed when the suite is enabled
   * @param testText description of the text
   * @param testFun function to evaluate
   */
  def ctest(testText: String)(testFun: => Unit): Unit = {
    if (enabled) {
      registerTest(testText) {testFun}
    } else {
      registerIgnoredTest(testText) {testFun}
    }
  }

}

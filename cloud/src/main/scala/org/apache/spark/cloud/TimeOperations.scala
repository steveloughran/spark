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

import java.util.Locale

import org.apache.spark.Logging

/**
 * Trait to add timing to operations.
 */
private[cloud]trait TimeOperations extends Logging {

  /**
   * Convert a time in nanoseconds into a human-readable form for logging.
   * @param durationNanos duration in nanoseconds
   * @return a string describing the time
   */
  def toHuman(durationNanos: Long): String = {
    String.format(Locale.ENGLISH, "%,d ns", durationNanos.asInstanceOf[Object])
  }

  /**
   * Measure the duration of an operation, log it with the text.
   * @param operation operation description
   * @param testFun function to execute
   * @return the result
   */
  def duration[T](operation: String)(testFun: => T): T = {
    val start = nanos
    logInfo(s"Starting $operation")
    try {
      testFun
    } finally {
      val end = nanos()
      val d = end - start
      logInfo(s"Duration of $operation = ${toHuman(d)}")
    }
  }

  /**
   * Measure the duration of an operation, log it.
   * @param testFun function to execute
   * @return the result and the operation duration in nanos
   */
  def duration2[T](testFun: => T): (T, Long, Long) = {
    val start = nanos()
    try {
      var r = testFun
      val end = nanos()
      val d = end - start
      (r, start, d)
    } catch {
      case ex: Exception =>
        val end = nanos()
        val d = end - start
        logError(s"After ${toHuman(d)} ns: $ex", ex)
        throw ex
    }
  }

  /**
   * Time in nanoseconds.
   * @return the current time.
   */
  def nanos(): Long = {
    System.nanoTime()
  }
}

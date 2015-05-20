/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive

import com.esotericsoftware.kryo.Kryo
import org.apache.hadoop.hive.ql.exec.{Utilities, FunctionRegistry}
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkSqlSerializer

/**
 * Tests to validate packing and shim integration, including
 * multiple shading of transitive dependencies
 *
 * If these tests fail, nothing else stands a chance
 */
class PackagingSuite extends FunSuite {

  test("Load Function Registry") {
    FunctionRegistry.getFunctionNames
  }


  test("LocalKryo") {
    new Kryo()
  }

  test("SparkSqlSerializer") {
    val conf = new SparkConf()
    new SparkSqlSerializer(conf)
  }

  test("Utilites") {
    // force load of the class
    Utilities.MAPNAME.length()
  }

  test("HiveFunctionWrapper") {
    HiveFunctionWrapper("anything")
  }

  test("LoadShadedKryo") {
    HiveShim.loadHiveClass(HiveShim.kryoClassname)
  }

  test("LoadShadedInstantiator") {
    HiveShim.loadHiveClass(HiveShim.stdInstantiatorStrategyClassname)
  }

}

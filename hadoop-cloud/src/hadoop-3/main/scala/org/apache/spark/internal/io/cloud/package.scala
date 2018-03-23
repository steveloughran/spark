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

package org.apache.spark.internal.io

import org.apache.spark.sql.internal.SQLConf

/**
 * Package object to assist in switching to the Hadoop Hadoop 3
 * [[org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory]] factory
 * mechanism for dynamically loading committers for the destination stores.
 */
package object cloud {

  /**
   * Options for committer setup.
   * When applied to a spark configuration, this will
   * * Switch parquet to the parquet committer subclass which will
   * then bind to the factory committer.
   * * Set spark.sql.sources.commitProtocolClass to PathOutputCommitProtocol
   */
  val COMMITTER_BINDING_OPTIONS: Map[String, String] = Map(
    SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key ->
      classOf[BindingParquetOutputCommitter].getName,
    SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[PathOutputCommitProtocol].getName)

}

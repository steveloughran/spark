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

package org.apache.spark.sql.hive.thriftserver


import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.sql.hive.HiveContext

private[hive] class SparkSQLCLIService(hiveServer: HiveServer2, hiveContext: HiveContext)
  extends CLIService(hiveServer) {

  override protected def createSessionManager(hiveConf: HiveConf, server: HiveServer2):
  SessionManager = {
    new SparkSQLSessionManager(server, hiveContext)
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(hiveContext.sparkContext.version)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}


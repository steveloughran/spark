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

package org.apache.spark.sql.hive.orc

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.ql.io.orc.{OrcFile, Reader}
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.hive.HiveMetastoreTypes

private[orc] object OrcFileOperator extends Logging{

  var conf: Configuration = _

  def getFileReader(pathStr: String): Reader = {
    var conf = Some(new Configuration)
    getFileReader(pathStr, conf)  }

  def getFileReader(pathStr: String, config: Option[Configuration] ): Reader = {
    var conf = config
    if (conf == null) {
      conf = Some(new Configuration)
    }
    val fspath = new Path(pathStr)
    val fs = fspath.getFileSystem(conf.getOrElse(new Configuration))
    val orcFiles = listOrcFiles(pathStr, conf.getOrElse(new Configuration()))
    OrcFile.createReader(fs, orcFiles(0))
  }

  def getSchema(path: String, conf: Option[Configuration]): Seq[Attribute] = {
    val reader = getFileReader(path, conf)
    val readerInspector: StructObjectInspector = reader.getObjectInspector
      .asInstanceOf[StructObjectInspector]
    val schema = readerInspector.getTypeName
    val attrs = HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType].toAttributes
    attrs
  }

  def getObjectInspector(path: String, conf: Option[Configuration]): StructObjectInspector = {
    val reader = getFileReader(path, conf)
    val readerInspector: StructObjectInspector = reader.getObjectInspector
      .asInstanceOf[StructObjectInspector]
    readerInspector
  }

  def deletePath(pathStr: String, conf: Configuration) = {
    val fspath = new Path(pathStr)
    val fs = fspath.getFileSystem(conf)
    try {
      fs.delete(fspath, true)
    } catch {
      case e: IOException =>
        throw new IOException(
          s"Unable to clear output directory ${fspath.toString} prior"
            + s" to InsertIntoOrcTable:\n${e.toString}")
    }
  }

  def listOrcFiles(pathStr: String, conf: Configuration): Seq[Path] = {
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"orcFileOperator: Path $origPath is incorrectly formatted")
    }
    val path = origPath.makeQualified(fs)
    if (!fs.exists(path) || !fs.getFileStatus(path).isDir) {
      throw new IllegalArgumentException(
        s"orcFileOperator: path $path does not exist or is not a directory")
    }
    val nameP = new scala.util.matching.Regex( """part-r-(\d{1,})-(\d{1,}).orc""")
    val paths = fs.listStatus(path).map(_.getPath)
      .filter(_.getName.matches("""part-r-(\d{1,})-(\d{1,}).orc"""))
    if (paths == null || paths.size == 0) {
      throw new IllegalArgumentException(
        s"orcFileOperator: path $path does not have valid orc files matching the pattern")
    }
    logInfo("Qualified file list: ")
    paths.foreach{x=>logInfo(x.toString)}
    paths
  }

   def checkPath(pathStr: String,
             allowExisting: Boolean, conf: Configuration): Path = {
    if (pathStr == null) {
      throw new IllegalArgumentException("Unable to create OrcRelation: path is null")
    }
    val origPath = new Path(pathStr)
    val fs = origPath.getFileSystem(conf)
    if (fs == null) {
      throw new IllegalArgumentException(
        s"Unable to create OrcRelation: incorrectly formatted path $pathStr")
    }
    val path = origPath.makeQualified(fs)
    if (!allowExisting && fs.exists(path)) {
      sys.error(s"File $pathStr already exists.")
    }
    if (fs.exists(path) &&
      !fs.getFileStatus(path)
        .getPermission
        .getUserAction
        .implies(FsAction.READ_WRITE)) {
      throw new IOException(
        s"Unable to create OrcRelation: path $path not read-writable")
    }
    path
  }
}

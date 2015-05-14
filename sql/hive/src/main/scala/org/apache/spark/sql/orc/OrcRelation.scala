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
import java.util.{ArrayList, HashSet, List}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.permission.FsAction
import org.apache.hadoop.hive.ql.io.orc.{OrcProto, OrcFile, Reader}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructField=>HiveStructField, StructObjectInspector}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedException}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

import scala.collection.JavaConversions._





private[sql] case class OrcRelation(path: String,
                  @transient conf: Option[Configuration],
                  @transient sqlContext: SQLContext,
                  partitioningAttributes: Seq[Attribute] = Nil)
  extends LeafNode with MultiInstanceRelation {
  self: Product =>

  /** Attributes */
  override val output: Seq[Attribute] =  getSchema
  def getSchema: Seq[Attribute] = OrcFileOperator.getSchema(path, conf)

  override def newInstance() = OrcRelation(path, conf,
    sqlContext).asInstanceOf[this.type]

  // Equals must also take into account the output attributes so that we can distinguish between
  // different instances of the same relation,
  override def equals(other: Any) = other match {
    case p: OrcRelation =>
      p.path == path && p.output == output
    case _ => false
  }

  // what's this for??
 // override lazy val statistics = Statistics(sizeInBytes = sqlContext.defaultSizeInBytes)
}

private[sql] object OrcRelation {

  /**
   * Creates a new OrcRelation and underlying Orcfile for the given LogicalPlan. Note that
   * this is used inside [[org.apache.spark.sql.execution.SparkStrategies SparkStrategies]] to
   * create a resolved relation as a data sink for writing to a Orcfile. The relation is empty
   * but is initialized with attributes attached.
   *
   * @param pathString The directory the Orcfile will be stored in.
   * @param child The child node that will be used for extracting the schema.
   * @param conf A configuration to be used.
   * @return An empty OrcRelation with inferred metadata.
   */
  def create(pathString: String,
             child: LogicalPlan,
             conf: Configuration,
             sqlContext: SQLContext): OrcRelation = {

    if (!child.resolved) {
      throw new UnresolvedException[LogicalPlan](
        child,
        "Attempt to create Orc table from unresolved child (when schema is not available)")
    }
    createEmpty(pathString, child.output, false, conf, sqlContext)
  }

  /**
   * Creates an empty orcRelation with output attached.
   *
   * @param pathString The directory the Orcfile will be stored in.
   * @param attributes The schema of the relation.
   * @param conf A configuration to be used.
   * @return An empty OrcRelation.
   */
  def createEmpty(pathString: String,
     attributes: Seq[Attribute],
     allowExisting: Boolean,
     conf: Configuration,
     sqlContext: SQLContext): OrcRelation = {

    val path = OrcFileOperator.checkPath(pathString, allowExisting, conf)
    new OrcRelation(path.toString, Some(conf), sqlContext) {
      override def getSchema: Seq[Attribute] = attributes
    }
  }
  var jobConf: Configuration = _
}

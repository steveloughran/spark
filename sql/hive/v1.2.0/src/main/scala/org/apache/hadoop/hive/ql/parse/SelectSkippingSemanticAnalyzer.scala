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
package org.apache.hadoop.hive.ql.parse

import org.apache.hadoop.hive.conf.HiveConf

/**
 * Subclass [[SemanticAnalyzer]] to tweak `analyzeInternal` slightly
 * @param conf hive configuration
 */
class SelectSkippingSemanticAnalyzer(conf: HiveConf) extends SemanticAnalyzer(conf) {

  override def analyzeInternal(ast: ASTNode) {
    // A hack to intercept the SemanticAnalyzer.analyzeInternal,
    // to ignore the SELECT clause of the CTAS
    // HIVE-1.2

    analyzeCreateTable(ast, this.getQB, null)
  }
}

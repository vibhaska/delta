/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical

/**
 * ALTER TABLE ... ADD CONSTRAINT command, as parsed from SQL
 */
case class AlterTableAddConstraintStatement(
    tableName: Seq[String],
    constraintName: String,
    expr: String) extends ParsedStatement {
  // TODO: extend LeafParsedStatement when new Spark version released, now fails on OSS Delta build
  override def children: Seq[LogicalPlan] = Nil

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}

/**
 * ALTER TABLE ... DROP CONSTRAINT command, as parsed from SQL
 */
case class AlterTableDropConstraintStatement(
    tableName: Seq[String],
    constraintName: String) extends ParsedStatement {
  // TODO: extend LeafParsedStatement when new Spark version released, now fails on OSS Delta build
  override def children: Seq[LogicalPlan] = Nil

  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}

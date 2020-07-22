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

package org.apache.spark.sql.delta.oci

import java.io.FileNotFoundException

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.oci.{FileOnOCI, QueryTestOnOCI}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test.SharedSparkSession

trait DescribeDeltaDetailSuiteBaseOnOCI extends QueryTestOnOCI
    with SharedSparkSession {

  import testImplicits._

  protected def checkResult(
    result: DataFrame,
    expected: Seq[Any],
    columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      Seq(Row(expected: _*))
    )
  }

  def describeDeltaDetailTest(f: FileOnOCI => String): Unit = {
    val tempDir = createTempDirOnOCI()
    try {
      Seq(1 -> 1).toDF("column1", "column2")
        .write
        .format("delta")
        .partitionBy("column1")
        .save(tempDir.toString())
      checkResult(
        sql(s"DESCRIBE DETAIL ${f(tempDir)}"),
        Seq("delta", Array("column1"), 1),
        Seq("format", "partitionColumns", "numFiles"))
    } finally {
      deleteRecursivelyOnOCI(tempDir)
    }
  }

  test("delta table: path") {
    describeDeltaDetailTest(f => s"'${f.toString()}'")
  }

  test("delta table: delta table identifier") {
    describeDeltaDetailTest(f => s"delta.`${f.toString()}`")
  }

  test("non-delta table: table name") {
    withTable("describe_detail") {
      sql(
        """
          |CREATE TABLE describe_detail(column1 INT, column2 INT)
          |USING parquet
          |PARTITIONED BY (column1)
          |COMMENT "this is a table comment"
        """.stripMargin)
      sql(
        """
          |INSERT INTO describe_detail VALUES(1, 1)
        """.stripMargin
      )
      checkResult(
        sql("DESCRIBE DETAIL describe_detail"),
        Seq("parquet", Array("column1")),
        Seq("format", "partitionColumns"))
    }
  }

  test("non-delta table: path") {
    val tempDir_D = createTempDirOnOCI()
    val tempDir = tempDir_D.toString
    try {
      Seq(1 -> 1).toDF("column1", "column2")
        .write
        .format("parquet")
        .partitionBy("column1")
        .mode("overwrite")
        .save(tempDir)
      checkResult(
        sql(s"DESCRIBE DETAIL '$tempDir'"),
        Seq(tempDir),
        Seq("location"))
    } finally {
      deleteRecursivelyOnOCI(tempDir_D)
    }
  }

  test("non-delta table: path doesn't exist") {
    val tempDir = createTempDirOnOCI()
    deleteOnOCI(tempDir)
    val e = intercept[FileNotFoundException] {
      sql(s"DESCRIBE DETAIL '$tempDir'")
    }
    assert(e.getMessage.contains(tempDir.toString))
  }

  test("delta table: table name") {
    withTable("describe_detail") {
      sql(
        """
          |CREATE TABLE describe_detail(column1 INT, column2 INT)
          |USING delta
          |PARTITIONED BY (column1)
          |COMMENT "describe a non delta table"
        """.stripMargin)
      sql(
        """
          |INSERT INTO describe_detail VALUES(1, 1)
        """.stripMargin
      )
      checkResult(
        sql("DESCRIBE DETAIL describe_detail"),
        Seq("delta", Array("column1"), 1),
        Seq("format", "partitionColumns", "numFiles"))
    }
  }

  test("delta table: create table on an existing delta log") {
    val tempDir = createTempDirOnOCI().toString
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("delta")
      .partitionBy("column1")
      .mode("overwrite")
      .save(tempDir)
    val tblName1 = "tbl_name1"
    val tblName2 = "tbl_name2"
    withTable(tblName1, tblName2) {
      sql(s"CREATE TABLE $tblName1 USING DELTA LOCATION '$tempDir'")
      sql(s"CREATE TABLE $tblName2 USING DELTA LOCATION '$tempDir'")
      checkResult(
        sql(s"DESCRIBE DETAIL $tblName1"),
        Seq(s"default.$tblName1"),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL $tblName2"),
        Seq(s"default.$tblName2"),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL delta.`$tempDir`"),
        Seq(null),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL '$tempDir'"),
        Seq(null),
        Seq("name"))
    }
  }
}

class DescribeDeltaDetailSuiteOnOCI
  extends DescribeDeltaDetailSuiteBaseOnOCI with DeltaSQLCommandTest

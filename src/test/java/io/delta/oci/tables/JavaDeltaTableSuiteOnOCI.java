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

package io.delta.oci.tables;

import java.util.Arrays;
import java.util.List;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.oci.JavaTestUtilsOnOCI;
import org.apache.spark.sql.test.*;
import org.apache.spark.sql.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JavaDeltaTableSuiteOnOCI {

  private transient TestSparkSession spark;
  private transient String input;
  private transient Boolean runTest;
  private transient JavaTestUtilsOnOCI javaTestUtils;


  @Before
  public void setUp() {
    // Trigger static initializer of TestData
    spark = new TestSparkSession();
    javaTestUtils = new JavaTestUtilsOnOCI();
    javaTestUtils.setLogStoreClass(spark.sparkContext().getConf());
    runTest = javaTestUtils.getRunTest(spark.sparkContext().getConf());
  }

  @After
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testAPI() {
    if (runTest) {
      input = javaTestUtils.createTempDirOnOCI(System.getProperty("java.io.tmpdir"),
              "spark", spark).toString();
      List<String> data = Arrays.asList("hello", "world");
      Dataset<Row> dataDF = spark.createDataset(data, Encoders.STRING()).toDF();
      List<Row> dataRows = dataDF.collectAsList();
      dataDF.write().format("delta").mode("overwrite").save(input);

      // Test creating DeltaTable by path
      DeltaTable table1 = DeltaTable.forPath(spark, input);
      QueryTest$.MODULE$.checkAnswer(table1.toDF(), dataRows);

      // Test creating DeltaTable by path picks up active SparkSession
      DeltaTable table2 = DeltaTable.forPath(input);
      QueryTest$.MODULE$.checkAnswer(table2.toDF(), dataRows);


      // Test DeltaTable.as() creates subquery alias
      QueryTest$.MODULE$.checkAnswer(table2.as("tbl").toDF().select("tbl.value"), dataRows);

      // Test DeltaTable.isDeltaTable() is true for a Delta file path.
      Assert.assertTrue(DeltaTable.isDeltaTable(input));
    } else {
      javaTestUtils.logConsole(ignoreTestAPI());
      spark.logInfo(this::ignoreTestAPI);
    }
  }

  private String ignoreTestAPI() {
    return javaTestUtils.testIgnoreMessage("JavaDeltaTableSuiteOnOCI::testAPI");
  }
}

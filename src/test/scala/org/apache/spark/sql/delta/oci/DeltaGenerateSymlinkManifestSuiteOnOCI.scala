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

import java.io.File
import java.net.URI

import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
import org.apache.spark.sql.delta.hooks.GenerateSymlinkManifest
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, Snapshot, SymlinkManifestFailureTestFileSystem}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.oci.{FileOnOCI, QueryTestOnOCI}
import org.apache.spark.sql.test.SharedSparkSession

class DeltaGenerateSymlinkManifestSuiteOnOCI
  extends DeltaGenerateSymlinkManifestSuiteBaseOnOCI
  with DeltaSQLCommandTest

trait DeltaGenerateSymlinkManifestSuiteBaseOnOCI extends QueryTestOnOCI
  with SharedSparkSession {

  import testImplicits._

  test("basic case: SQL command - path-based table") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").mode("overwrite").save(tablePath.toString)

      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      // Create a Delta table and call the scala api for generating manifest files
      spark.sql(s"GENERATE symlink_ForMat_Manifest FOR TABLE delta.`${tablePath.getAbsolutePath}`")
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  // Ignore for OCI Object Store
  ignore("basic case: SQL command - name-based table") {
    withTable("deltaTable") {
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").saveAsTable("deltaTable")

      val tableId = TableIdentifier("deltaTable")
      val tablePath = new File(spark.sessionState.catalog.getTableMetadata(tableId).location)
      println(tablePath.toString) // scalastyle:off println
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      spark.sql(s"GENERATE symlink_ForMat_Manifest FOR TABLE deltaTable")
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("basic case: SQL command - throw error on bad tables") {
    var e: Exception = intercept[AnalysisException] {
      spark.sql("GENERATE symlink_format_manifest FOR TABLE nonExistentTable")
    }
    assert(e.getMessage.contains("not found"))

    withTable("nonDeltaTable") {
      spark.range(2).write.format("parquet").saveAsTable("nonDeltaTable")
      e = intercept[AnalysisException] {
        spark.sql("GENERATE symlink_format_manifest FOR TABLE nonDeltaTable")
      }
      assert(e.getMessage.contains("only supported for Delta"))
    }
  }

  test("basic case: SQL command - throw error on non delta table paths") {
    withTempDirOnOCI { dir =>
      var e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$dir`")
      }

      assert(e.getMessage.contains("not found") ||
        e.getMessage.contains("only supported for Delta"))

      spark.range(2).write.format("parquet").mode("overwrite").save(dir.toString)

      e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE delta.`$dir`")
      }
      assert(e.getMessage.contains("table not found") ||
        e.getMessage.contains("only supported for Delta"))

      e = intercept[AnalysisException] {
        spark.sql(s"GENERATE symlink_format_manifest FOR TABLE parquet.`$dir`")
      }
      assert(e.getMessage.contains("not found"))
    }
  }

  test("basic case: SQL command - throw error on unsupported mode") {
    withTempDirOnOCI { tablePath =>
      spark.range(2).write.format("delta").save(tablePath.getAbsolutePath)
      val e = intercept[IllegalArgumentException] {
        spark.sql(s"GENERATE xyz FOR TABLE delta.`${tablePath.getAbsolutePath}`")
      }
      assert(e.toString.contains("not supported"))
      DeltaGenerateCommand.modeNameToGenerationFunc.keys.foreach { modeName =>
        assert(e.toString.contains(modeName))
      }
    }
  }

  test("basic case: Scala API - path-based table") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").mode("overwrite").save(tablePath.toString)

      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      // Create a Delta table and call the scala api for generating manifest files
      val deltaTable = io.delta.tables.DeltaTable.forPath(tablePath.getAbsolutePath)
      deltaTable.generate("symlink_format_manifest")
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  // Ignore for OCI Object Store
  ignore("basic case: Scala API - name-based table") {
    withTable("deltaTable") {
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 7))
        .write.format("delta").saveAsTable("deltaTable")

      val tableId = TableIdentifier("deltaTable")
      val tablePath = new File(spark.sessionState.catalog.getTableMetadata(tableId).location)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      val deltaTable = io.delta.tables.DeltaTable.forName("deltaTable")
      deltaTable.generate("symlink_format_manifest")
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)
    }
  }


  test ("full manifest: non-partitioned table") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      def write(parallelism: Int): Unit = {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          .write.format("delta").mode("overwrite").save(tablePath.toString)
      }

      write(7)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)

      // Reduce files
      write(5)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 7)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 5)

      // Remove all data
      spark.emptyDataset[Int].write.format("delta").mode("overwrite").save(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 5)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 1)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)

      // delete all data
      write(5)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 1)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.delete()
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
    }
  }

  test("full manifest: partitioned table") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      def write(parallelism: Int, partitions1: Int, partitions2: Int): Unit = {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism)).toDF("value")
          .withColumn("part1", $"value" % partitions1)
          .withColumn("part2", $"value" % partitions2)
          .write.format("delta").partitionBy("part1", "part2")
          .mode("overwrite").save(tablePath.toString)
      }

      write(10, 10, 10)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)
      generateSymlinkManifest(tablePath.toString)
      // 10 files each in ../part1=X/part2=X/ for X = 0 to 9
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 100)

      // Reduce # partitions on both dimensions
      write(1, 1, 1)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 100)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 1)

      // Increase # partitions on both dimensions
      write(5, 5, 5)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 1)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 25)

      // Increase # partitions on only one dimension
      write(5, 10, 5)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 25)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 50)

      // Remove all data
      spark.emptyDataset[Int].toDF("value")
        .withColumn("part1", $"value" % 10)
        .withColumn("part2", $"value" % 10)
        .write.format("delta").mode("overwrite").save(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 50)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)

      // delete all data
      write(5, 5, 5)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 25)
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.delete()
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 0)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
    }
  }

  test("incremental manifest: table property controls post commit manifest generation") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      def writeWithIncrementalManifest(enabled: Boolean, numFiles: Int): Unit = {
        withIncrementalManifest(tablePath, enabled) {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
            .write.format("delta").mode("overwrite").save(tablePath.toString)
        }
      }

      writeWithIncrementalManifest(enabled = false, numFiles = 1)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      // Enabling it should automatically generate manifest files
      writeWithIncrementalManifest(enabled = true, numFiles = 2)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 2)

      // Disabling it should stop updating existing manifest files
      writeWithIncrementalManifest(enabled = false, numFiles = 3)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 2)
    }
  }

  test("incremental manifest: unpartitioned table") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      def write(numFiles: Int): Unit = withIncrementalManifest(tablePath, enabled = true) {
        spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
          .write.format("delta").mode("overwrite").save(tablePath.toString)
      }

      write(1)
      // first write won't generate automatic manifest as mode enable after first write
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      // Increase files
      write(7)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)

      // Reduce files
      write(5)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 5)

      // Remove all data
      spark.emptyDataset[Int].write.format("delta").mode("overwrite").save(tablePath.toString)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 1)
    }
  }

  test("incremental manifest: partitioned table") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)

      def writePartitioned(parallelism: Int, numPartitions1: Int, numPartitions2: Int): Unit = {
        withIncrementalManifest(tablePath, enabled = true) {
          val input =
            if (parallelism == 0) spark.emptyDataset[Int]
            else spark.createDataset(spark.sparkContext.parallelize(1 to 100, parallelism))
          input.toDF("value")
            .withColumn("part1", $"value" % numPartitions1)
            .withColumn("part2", $"value" % numPartitions2)
            .write.format("delta").partitionBy("part1", "part2")
            .mode("overwrite").save(tablePath.toString)
        }
      }

      writePartitioned(1, 1, 1)
      // Manifests wont be generated in the first write because `withIncrementalManifest` will
      // enable manifest generation only after the first write defines the table log.
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      writePartitioned(10, 10, 10)
      // 10 files each in ../part1=X/part2=X/ for X = 0 to 9 (so only 10 subdirectories)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 100)

      // Update such that 1 file is removed and 1 file is added in another partition
      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tablePath.toString)
      deltaTable.updateExpr("value = 1", Map("part1" -> "0", "value" -> "-1"))
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 100)

      // Delete such that 1 file is removed
      deltaTable.delete("value = -1")
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 99)

      // Reduce # partitions on both dimensions
      writePartitioned(1, 1, 1)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 1)

      // Increase # partitions on both dimensions
      writePartitioned(5, 5, 5)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 25)

      // Increase # partitions on only one dimension
      writePartitioned(5, 10, 5)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 50)

      // Remove all data
      writePartitioned(0, 1, 1)
      assert(spark.read.format("delta").load(tablePath.toString).count() == 0)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 0)
    }
  }

  test("incremental manifest: generate full manifest if manifest did not exist") {
    withTempDirOnOCI { tablePath =>

      def write(numPartitions: Int): Unit = {
        spark.range(0, 100, 1, 1).toDF("value").withColumn("part", $"value" % numPartitions)
          .write.format("delta").partitionBy("part").mode("append").save(tablePath.toString)
      }

      write(10)
      assertManifest(tablePath.toString, expectSameFiles = false, expectedNumFiles = 0)

      withIncrementalManifest(tablePath, enabled = true) {
        write(1)  // update only one partition
      }
      // Manifests should be generated for all partitions
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 11)
    }
  }

  test("incremental manifest: failure to generate manifest throws exception") {
    withTempDir { tablePath =>
      tablePath.delete()

      import SymlinkManifestFailureTestFileSystem._

      withSQLConf(
          s"fs.$SCHEME.impl" -> classOf[SymlinkManifestFailureTestFileSystem].getName,
          s"fs.$SCHEME.impl.disable.cache" -> "true",
          s"fs.AbstractFileSystem.$SCHEME.impl" ->
            classOf[SymlinkManifestFailureTestAbstractFileSystem].getName,
          s"fs.AbstractFileSystem.$SCHEME.impl.disable.cache" -> "true") {
        def write(numFiles: Int): Unit = withIncrementalManifest(tablePath, enabled = true) {
          spark.createDataset(spark.sparkContext.parallelize(1 to 100, numFiles))
            .write.format("delta").mode("overwrite").save(s"$SCHEME://$tablePath")
        }

        val manifestPath = new File(tablePath, GenerateSymlinkManifest.MANIFEST_LOCATION)
        require(!manifestPath.exists())
        write(1) // first write enables the property does not write any file
        require(!manifestPath.exists())

        val ex = catalyst.util.quietly {
          intercept[RuntimeException] { write(2) }
        }

        assert(ex.getMessage().contains(GenerateSymlinkManifest.name))
        assert(ex.getCause().toString.contains("Test exception"))
      }
    }
  }

  test("special partition column names") {

    def assertColNames(inputStr: String): Unit = withClue(s"input: $inputStr") {
      withTempDirOnOCI { tablePath =>
        deleteOnOCI(tablePath)
        val inputLines = inputStr.trim.stripMargin.trim.split("\n").toSeq
        require(inputLines.size > 0)
        val input = spark.read.json(inputLines.toDS)
        val partitionCols = input.schema.fieldNames
        val inputWithValue = input.withColumn("value", lit(1))

        inputWithValue.write.format("delta").partitionBy(partitionCols: _*).save(tablePath.toString)
        generateSymlinkManifest(tablePath.toString)
        assertManifest(tablePath.toString, expectSameFiles = true,
          expectedNumFiles = inputLines.size)
      }
    }

    intercept[AnalysisException] {
      assertColNames("""{ " " : 0 }""")
    }
    assertColNames("""{ "%" : 0 }""")
    assertColNames("""{ "a.b." : 0 }""")
    assertColNames("""{ "a/b." : 0 }""")
    assertColNames("""{ "a_b" : 0 }""")
    intercept[AnalysisException] {
      assertColNames("""{ "a b" : 0 }""")
    }
  }

  test("special partition column values") {
    withTempDirOnOCI { tablePath =>
      deleteOnOCI(tablePath)
      val inputStr = """
          |{ "part1" : 1,    "part2": "$0$", "value" : 1 }
          |{ "part1" : null, "part2": "_1_", "value" : 1 }
          |{ "part1" : 1,    "part2": "",    "value" : 1 }
          |{ "part1" : null, "part2": " ",   "value" : 1 }
          |{ "part1" : 1,    "part2": "  ",  "value" : 1 }
          |{ "part1" : null, "part2": "/",   "value" : 1 }
          |{ "part1" : 1,    "part2": null,  "value" : 1 }
          |"""
      val input = spark.read.json(inputStr.trim.stripMargin.trim.split("\n").toSeq.toDS)
      input.write.format("delta").partitionBy("part1", "part2").save(tablePath.toString)
      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 7)
    }
  }

  test("root table path with escapable chars like space") {
    withTempDirOnOCI { p =>
      val tablePath = new FileOnOCI(p, "path with space")
      spark.createDataset(spark.sparkContext.parallelize(1 to 100, 1)).toDF("value")
        .withColumn("part", $"value" % 2)
        .write.format("delta").partitionBy("part").save(tablePath.toString)

      generateSymlinkManifest(tablePath.toString)
      assertManifest(tablePath.toString, expectSameFiles = true, expectedNumFiles = 2)
    }
  }

  private def assertManifestFiles(tablePath: String, expectSameFiles: Boolean,
                                  expectedNumFiles: Int, manifestPathAsString: String,
                                  deltaSnapshot: Snapshot, fs: FileSystem): Unit = {
    val filesInManifest = spark.read.text(manifestPathAsString).select("value").as[String]
      .map { _.stripPrefix("file:") }.toDF("file")
    assert(filesInManifest.count() == expectedNumFiles)

    // Validate that files in the latest version of DeltaLog is same as those in the manifest
    val filesInLog = deltaSnapshot.allFiles.map { addFile =>
      // Note: this unescapes the relative path in `addFile`
      DeltaFileOperations.absolutePath(tablePath, addFile.path).toString
    }.toDF("file")
    if (expectSameFiles) {
      checkAnswer(filesInManifest, filesInLog.toDF())

      // Validate that each file in the manifest is actually present in table. This mainly checks
      // whether the file names in manifest are not escaped and therefore are readable directly
      // by Hadoop APIs.
      spark.read.text(manifestPathAsString).select("value").as[String].collect().foreach { p =>
        assert(fs.exists(new Path(p)), s"path $p in manifest not found in file system")
      }
    } else {
      assert(filesInManifest.as[String].collect().toSet != filesInLog.as[String].collect().toSet)
    }
  }

  /**
   * Assert that the manifest files in the table meet the expectations.
   * @param tabPath Path of the Delta table
   * @param expectSameFiles Expect that the manifest files contain the same data files
   *                        as the latest version of the table
   * @param expectedNumFiles Expected number of manifest files
   */
  def assertManifest(tabPath: String, expectSameFiles: Boolean, expectedNumFiles: Int): Unit = {
    val path = new Path(tabPath)
    val tablePath = new FileOnOCI(path)

    val deltaSnapshot = DeltaLog.forTable(spark, tablePath.toString).update()
    val manifestPath = new FileOnOCI(tablePath, GenerateSymlinkManifest.MANIFEST_LOCATION)

    if (!existsOnOCI(manifestPath)) {
      assert(expectedNumFiles == 0 && !expectSameFiles)
      return
    } else {
      // Validate the expected number of files are present in the manifest
      val manifestPathAsString = manifestPath.toString
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      assertManifestFiles(tablePath.toString, expectSameFiles, expectedNumFiles,
        manifestPathAsString, deltaSnapshot, fs)
    }

    // If there are partitioned files, make sure the partitions values read from them are the
    // same as those in the table.
    val partitionCols = deltaSnapshot.metadata.partitionColumns.map(x => s"`$x`")
    if (partitionCols.nonEmpty && expectSameFiles && expectedNumFiles > 0) {
      val partitionsInManifest = spark.read.text(manifestPath.toString)
        .selectExpr(partitionCols: _*).distinct()
      val partitionsInData = spark.read.format("delta").load(tablePath.toString)
        .selectExpr(partitionCols: _*).distinct()
      checkAnswer(partitionsInManifest, partitionsInData)
    }
  }

  protected def withIncrementalManifest(tablePath: FileOnOCI, enabled: Boolean)
                                       (func: => Unit): Unit = {
    if (existsOnOCI(tablePath)) {
      val latestMetadata = DeltaLog.forTable(spark, tablePath.path).update().metadata
      if (DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.fromMetaData(latestMetadata) != enabled) {
        spark.sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES(${DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.key}=$enabled)")
      }
    }
    func
  }

  protected def withIncrementalManifest(tablePath: File, enabled: Boolean)
                                       (func: => Unit): Unit = {
    if (tablePath.exists()) {
      val latestMetadata = DeltaLog.forTable(spark, tablePath).update().metadata
      if (DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.fromMetaData(latestMetadata) != enabled) {
        spark.sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES(${DeltaConfigs.SYMLINK_FORMAT_MANIFEST_ENABLED.key}=$enabled)")
      }
    }
    func
  }

  protected def generateSymlinkManifest(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tablePath)
    GenerateSymlinkManifest.generateFullManifest(spark, deltaLog)
  }
}

class SymlinkManifestFailureTestAbstractFileSystem(
    uri: URI,
    conf: org.apache.hadoop.conf.Configuration)
  extends org.apache.hadoop.fs.DelegateToFileSystem(
    uri,
    new SymlinkManifestFailureTestFileSystem,
    conf,
    SymlinkManifestFailureTestFileSystem.SCHEME,
    false) {

  // Implementation copied from RawLocalFs
  import org.apache.hadoop.fs.local.LocalConfigKeys
  import org.apache.hadoop.fs._

  override def getUriDefaultPort(): Int = -1
  override def getServerDefaults(): FsServerDefaults = LocalConfigKeys.getServerDefaults()
  override def isValidName(src: String): Boolean = true
}



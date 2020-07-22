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

import java.io.{File, FileInputStream, OutputStream}
import java.net.URI
import java.util.UUID

import org.apache.spark.sql.delta.actions.{AddFile, InvalidProtocolVersionException, Protocol}
import org.apache.spark.sql.delta.sources.{DeltaSQLConf, DeltaSourceOffset}
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations, DeltaOptions}
import org.apache.spark.sql.delta.oci.DeltaSourceSuiteBaseOnOCI
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.oci.FileOnOCI
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException, Trigger}
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{ManualClock, Utils}

class DeltaSourceSuiteOnOCI extends DeltaSourceSuiteBaseOnOCI {

  import testImplicits._

  private def withTempDirOnOCIs(f: (FileOnOCI, FileOnOCI, FileOnOCI) => Unit): Unit = {
    withTempDirOnOCI { file1 =>
      withTempDirOnOCI { file2 =>
        withTempDirOnOCI { file3 =>
          f(file1, file2, file3)
        }
      }
    }
  }

  test("no schema should throw an exception") {
    withTempDirOnOCI { inputDir =>
      mkdirOnOCI(new FileOnOCI(inputDir, "_delta_log"))
      val e = intercept[AnalysisException] {
        spark.readStream
          .format("delta")
          .load(inputDir.getCanonicalPath)
      }
      for (msg <- Seq("Table schema is not set", "CREATE TABLE")) {
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("disallow user specified schema") {
    withTempDirOnOCI { inputDir =>
      mkdirOnOCI(new FileOnOCI(inputDir, "_delta_log"))
      val e = intercept[AnalysisException] {
        spark.readStream
          .schema(StructType.fromDDL("a INT, b STRING"))
          .format("delta")
          .load(inputDir.getCanonicalPath)
      }
      for (msg <- Seq("Delta does not support specifying the schema at read time")) {
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("basic") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AddToReservoir(inputDir, Seq("drop4", "keep5", "keep6").toDF),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2", "keep5", "keep6"),
        AddToReservoir(inputDir, Seq("keep7", "drop8", "keep9").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2", "keep5", "keep6", "keep7", "keep9")
      )
    }
  }

  test("allow to change schema before staring a streaming query") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF("id")
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      withMetadata(deltaLog, StructType.fromDDL("id STRING, value STRING"))

      (5 until 10).foreach { i =>
        val v = Seq(i.toString -> i.toString).toDF("id", "value")
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      val expected = (
          (0 until 5).map(_.toString -> null) ++ (5 until 10).map(_.toString).map(x => x -> x)
        ).toDF("id", "value").collect()
      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer(expected: _*)
      )
    }
  }

  testQuietly("disallow to change schema after starting a streaming query") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer((0 until 5).map(_.toString): _*),
        AssertOnQuery { _ =>
          withMetadata(deltaLog, StructType.fromDDL("id LONG, value STRING"))
          true
        },
        ExpectFailure[IllegalStateException](t =>
          assert(t.getMessage.contains("Detected schema change")))
      )
    }
  }

  test("maxFilesPerTrigger") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: metadata checkpoint") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 20).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxFilesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 20)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxFilesPerTriggerTest"), (0 until 20).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxFilesPerTrigger: change and restart") {
    withTempDirOnOCIs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 10).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 10)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 10).map(_.toString).toDF())
      } finally {
        q.stop()
      }

      (10 until 20).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q2 = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q2.processAllAvailable()
        val progress = q2.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 2)
        }

        checkAnswer(
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 20).map(_.toString).toDF())
      } finally {
        q2.stop()
      }
    }
  }

  testQuietly("maxFilesPerTrigger: invalid parameter") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      Seq(0, -1, "string").foreach { invalidMaxFilesPerTrigger =>
        val e = intercept[StreamingQueryException] {
          spark.readStream
            .format("delta")
            .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, invalidMaxFilesPerTrigger.toString)
            .load(inputDir.getCanonicalPath)
            .writeStream
            .format("console")
            .start()
            .processAllAvailable()
        }
        assert(e.getCause.isInstanceOf[IllegalArgumentException])
        for (msg <- Seq("Invalid", DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "positive")) {
          assert(e.getCause.getMessage.contains(msg))
        }
      }
    }
  }

  test("maxFilesPerTrigger: ignored when using Trigger.Once") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .trigger(Trigger.Once)
        .queryName("triggerOnceTest")
        .start()
      try {
        assert(q.awaitTermination(streamingTimeout.toMillis))
        assert(q.recentProgress.count(_.numInputRows != 0) == 1) // only one trigger was run
        checkAnswer(sql("SELECT * from triggerOnceTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxBytesPerTrigger: process at least one file") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxBytesPerTrigger: metadata checkpoint") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 20).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 20)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 20).map(_.toString).toDF)
      } finally {
        q.stop()
      }
    }
  }

  test("maxBytesPerTrigger: change and restart") {
    withTempDirOnOCIs { (inputDir, outputDir, checkpointDir) =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 10).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 10)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 10).map(_.toString).toDF())
      } finally {
        q.stop()
      }

      (10 until 20).foreach { i =>
        val v = Seq(i.toString).toDF()
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q2 = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "100g")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(outputDir.getCanonicalPath)
      try {
        q2.processAllAvailable()
        val progress = q2.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 1)
        progress.foreach { p =>
          assert(p.numInputRows === 10)
        }

        checkAnswer(
          spark.read.format("delta").load(outputDir.getAbsolutePath),
          (0 until 20).map(_.toString).toDF())
      } finally {
        q2.stop()
      }
    }
  }

  testQuietly("maxBytesPerTrigger: invalid parameter") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      Seq(0, -1, "string").foreach { invalidMaxBytesPerTrigger =>
        val e = intercept[StreamingQueryException] {
          spark.readStream
            .format("delta")
            .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, invalidMaxBytesPerTrigger.toString)
            .load(inputDir.getCanonicalPath)
            .writeStream
            .format("console")
            .start()
            .processAllAvailable()
        }
        assert(e.getCause.isInstanceOf[IllegalArgumentException])
        for (msg <- Seq("Invalid", DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "size")) {
          assert(e.getCause.getMessage.contains(msg))
        }
      }
    }
  }

  test("maxBytesPerTrigger: max bytes and max files together") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      (0 until 5).foreach { i =>
        val v = Seq(i.toString).toDF
        v.write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      val q = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "1") // should process a file at a time
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "100gb")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q.processAllAvailable()
        val progress = q.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q.stop()
      }

      val q2 = spark.readStream
        .format("delta")
        .option(DeltaOptions.MAX_FILES_PER_TRIGGER_OPTION, "2")
        .option(DeltaOptions.MAX_BYTES_PER_TRIGGER_OPTION, "1b")
        .load(inputDir.getCanonicalPath)
        .writeStream
        .format("memory")
        .queryName("maxBytesPerTriggerTest")
        .start()
      try {
        q2.processAllAvailable()
        val progress = q2.recentProgress.filter(_.numInputRows != 0)
        assert(progress.length === 5)
        progress.foreach { p =>
          assert(p.numInputRows === 1)
        }
        checkAnswer(sql("SELECT * from maxBytesPerTriggerTest"), (0 until 5).map(_.toString).toDF)
      } finally {
        q2.stop()
      }
    }
  }

  test("unknown sourceVersion value") {
    val json =
      s"""
         |{
         |  "sourceVersion": ${Long.MaxValue},
         |  "reservoirVersion": 1,
         |  "index": 1,
         |  "isStartingVersion": true
         |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    assert(e.getMessage.contains("Please upgrade your Spark"))
  }

  test("invalid sourceVersion value") {
    val json =
      """
        |{
        |  "sourceVersion": "foo",
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("foo", "invalid")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("missing sourceVersion") {
    val json =
      """
        |{
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("Cannot find", "sourceVersion")) {
      assert(e.getMessage.contains(msg))
    }
  }

  test("unmatched reservoir id") {
    val json =
      s"""
        |{
        |  "reservoirId": "${UUID.randomUUID().toString}",
        |  "sourceVersion": 1,
        |  "reservoirVersion": 1,
        |  "index": 1,
        |  "isStartingVersion": true
        |}
      """.stripMargin
    val e = intercept[IllegalStateException] {
      DeltaSourceOffset(UUID.randomUUID().toString, SerializedOffset(json))
    }
    for (msg <- Seq("delete", "checkpoint", "restart")) {
      assert(e.getMessage.contains(msg))
    }
  }

  testQuietly("recreate the reservoir should fail the query") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)
        .filter($"value" contains "keep")

      testStream(df)(
        AddToReservoir(inputDir, Seq("keep1", "keep2", "drop3").toDF),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("keep1", "keep2"),
        StopStream,
        AssertOnQuery { _ =>
          deleteRecursivelyOnOCI(inputDir)
          val deltaLog = DeltaLog.forTable(spark, inputDir.path)
          // All Delta tables in tests use the same tableId by default. Here we pass a new tableId
          // to simulate a new table creation in production
          withMetadata(deltaLog, StructType.fromDDL("value STRING"), tableId = Some("tableId-1234"))
          true
        },
        StartStream(),
        ExpectFailure[IllegalStateException] { e =>
          for (msg <- Seq("delete", "checkpoint", "restart")) {
            assert(e.getMessage.contains(msg))
          }
        }
      )
    }
  }

  test("excludeRegex works and doesn't mess up offsets across restarts - parquet version") {
    withTempDirOnOCI { inputDir =>
      val chk = new FileOnOCI(inputDir, "_checkpoint").toString

      def excludeReTest(s: Option[String], expected: String*): Unit = {
        val dfr = spark.readStream
          .format("delta")
        s.foreach(regex => dfr.option(DeltaOptions.EXCLUDE_REGEX_OPTION, regex))
        val df = dfr.load(inputDir.getCanonicalPath).groupBy('value).count
        testStream(df, OutputMode.Complete())(
          StartStream(checkpointLocation = chk),
          AssertOnQuery { sq => sq.processAllAvailable(); true },
          CheckLastBatch(expected.map((_, 1)): _*),
          StopStream
        )
      }

      val deltaLog = DeltaLog.forTable(spark, inputDir.path)

      def writeFile(name: String, content: String): AddFile = {
        writeOnOCI(new FileOnOCI(inputDir, name), content)
        AddFile(name, Map.empty, content.length, System.currentTimeMillis(), dataChange = true)
      }

      def commitFiles(files: AddFile*): Unit = {
        deltaLog.startTransaction().commit(files, DeltaOperations.ManualUpdate)
      }

      Seq("abc", "def").toDF().write.format("delta").save(inputDir.getAbsolutePath)
      commitFiles(
        writeFile("batch1-ignore-file1", "ghi"),
        writeFile("batch1-ignore-file2", "jkl")
      )
      excludeReTest(Some("ignore"), "abc", "def")
    }
  }

  testQuietly("excludeRegex throws good error on bad regex pattern") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val e = intercept[StreamingQueryException] {
        spark.readStream
          .format("delta")
          .option(DeltaOptions.EXCLUDE_REGEX_OPTION, "[abc")
          .load(inputDir.getCanonicalPath)
          .writeStream
          .format("console")
          .start()
          .awaitTermination()
      }.cause
      assert(e.isInstanceOf[IllegalArgumentException])
      assert(e.getMessage.contains(DeltaOptions.EXCLUDE_REGEX_OPTION))
    }
  }

  test("a fast writer should not starve a Delta source") {
    val deltaPath = createTempDirOnOCI().getCanonicalPath
    val checkpointPath = createTempDirOnOCI().getCanonicalPath
    val writer = spark.readStream
      .format("rate")
      .load()
      .writeStream
      .format("delta")
      .option("checkpointLocation", checkpointPath)
      .start(deltaPath)
    try {
      eventually(timeout(streamingTimeout)) {
        assert(spark.read.format("delta").load(deltaPath).count() > 0)
      }
      val testTableName = "delta_source_test"
      withTable(testTableName) {
        val reader = spark.readStream
          .format("delta")
          .load(deltaPath)
          .writeStream
          .format("memory")
          .queryName(testTableName)
          .start()
        try {
          eventually(timeout(streamingTimeout)) {
            assert(spark.table(testTableName).count() > 0)
          }
        } finally {
          reader.stop()
        }
      }
    } finally {
      writer.stop()
    }
  }

  test("start from corrupt checkpoint") {
    withTempDirOnOCI { inputDir =>
      val path = inputDir.getAbsolutePath
      for (i <- 1 to 5) {
        Seq(i).toDF("id").write.mode("append").format("delta").save(path)
      }
      val deltaLog = DeltaLog.forTable(spark, path)
      withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> "1") {
        deltaLog.checkpoint()
      }
      Seq(6).toDF("id").write.mode("append").format("delta").save(path)
      val checkpoints = listFilesOnOCI(new FileOnOCI(deltaLog.logPath))
        .filter(f => FileNames.isCheckpointFile(new Path(f.getAbsolutePath)))
      deleteOnOCI(checkpoints.last)

      val df = spark.readStream
        .format("delta")
        .load(inputDir.getCanonicalPath)

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer(1, 2, 3, 4, 5, 6),
        StopStream
      )
    }
  }

  test("SC-11561: can consume new data without update") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream.format("delta").load(inputDir.getCanonicalPath)

      // clear the cache so that the writer creates its own DeltaLog instead of reusing the reader's
      DeltaLog.clearCache()
      (0 until 3).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      // check that reader consumed new data without updating its DeltaLog
      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("0", "1", "2")
      )
      assert(deltaLog.snapshot.version == 0)

      (3 until 5).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("delta").save(deltaLog.dataPath.toString)
      }

      // check that reader consumed new data without update despite checkpoint
      val writersLog = DeltaLog.forTable(spark, inputDir.path)
      writersLog.checkpoint()
      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswer("0", "1", "2", "3", "4")
      )
      assert(deltaLog.snapshot.version == 0)
    }
  }

  test("SC-11561: can delete old files of a snapshot without update") {
    withTempDirOnOCI { inputDir =>
      val deltaLog = DeltaLog.forTable(spark, inputDir.path)
      withMetadata(deltaLog, StructType.fromDDL("value STRING"))

      val df = spark.readStream.format("delta").load(inputDir.getCanonicalPath)

      // clear the cache so that the writer creates its own DeltaLog instead of reusing the reader's
      DeltaLog.clearCache()
      val clock = new ManualClock(System.currentTimeMillis())
      val writersLog = DeltaLog.forTable(spark, inputDir.path, clock)
      (0 until 3).foreach { i =>
        Seq(i.toString).toDF("value")
          .write.mode("append").format("delta").save(inputDir.getCanonicalPath)
      }

      // Create a checkpoint so that logs before checkpoint can be expired and deleted
      writersLog.checkpoint()

      testStream(df)(
        StartStream(Trigger.ProcessingTime("10 seconds"), new StreamManualClock),
        AdvanceManualClock(10 * 1000L),
        CheckLastBatch("0", "1", "2"),
        Assert {
          val defaultLogRetentionMillis = DeltaConfigs.getMilliSeconds(
            IntervalUtils.safeStringToInterval(
              UTF8String.fromString(DeltaConfigs.LOG_RETENTION.defaultValue)))
          clock.advance(defaultLogRetentionMillis + 100000000L)

          // Delete all logs before checkpoint
          writersLog.cleanUpExpiredLogs()

          // Check that the first few log files have been deleted
          val logPath = new FileOnOCI(inputDir, "_delta_log")
          val logVersions = listFilesOnOCI(logPath).map(_.getName)
              .filter(_.endsWith(".json"))
              .map(_.stripSuffix(".json").toInt)

          !logVersions.contains(0) && !logVersions.contains(1)
        },
        Assert {
          (3 until 5).foreach { i =>
            Seq(i.toString).toDF("value")
              .write.mode("append").format("delta").save(inputDir.getCanonicalPath)
          }
          true
        },
        // can process new data without update, despite that previous log files have been deleted
        AdvanceManualClock(10 * 1000L),
        AdvanceManualClock(10 * 1000L),
        CheckLastBatch("3", "4")
      )
      assert(deltaLog.snapshot.version == 0)
    }
  }

  test("Delta sources don't write offsets with null json") {
    withTempDirOnOCIs { (inputDir, outputDir, checkpointDir) =>
      Seq(1, 2, 3).toDF("x").write.format("delta").save(inputDir.toString)

      val df = spark.readStream.format("delta").load(inputDir.toString)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
      stream.processAllAvailable()
      val offsetOciFile = new FileOnOCI(checkpointDir, "/offsets/0")

      // Make sure JsonUtils doesn't serialize it as null
      val deltaSourceOffsetLine =
        fromFileOnOCI(offsetOciFile).getLines.toSeq.last
      val deltaSourceOffset = JsonUtils.fromJson[DeltaSourceOffset](deltaSourceOffsetLine)
      assert(deltaSourceOffset.json != null, "Delta sources shouldn't write null json field")

      // Make sure OffsetSeqLog won't choke on the offset we wrote
      withTempDirOnOCI { logPath =>
        val seqLog = new OffsetSeqLog(spark, logPath.toString) {
          val offsetSeq = this.deserialize(openInputStreamOnOCI(offsetOciFile))
          val out = new OutputStream() { override def write(b: Int): Unit = { } }
          this.serialize(offsetSeq, out)
        }
      }

      stream.stop()
    }
  }

  test("Delta source advances with non-data inserts") {
    withTempDirOnOCIs { (inputDir, outputDir, checkpointDir) =>
      Seq(1L, 2L, 3L).toDF("x").write.format("delta").save(inputDir.toString)

      val df = spark.readStream.format("delta").load(inputDir.toString)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .start(outputDir.toString)
      try {
        stream.processAllAvailable()

        val deltaLog = DeltaLog.forTable(spark, inputDir.toString)
        for(i <- 1 to 3) {
          deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
          stream.processAllAvailable()
        }

        val fs = deltaLog.dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        for (version <- 0 to 3) {
          val possibleFiles = Seq(
            f"/$version%020d.checkpoint.parquet",
            f"/$version%020d.json",
            f"/$version%020d.crc"
          ).map { name => new Path(inputDir.toString + "/_delta_log" + name) }
          for (logFilePath <- possibleFiles) {
            if (fs.exists(logFilePath)) {
              // The cleanup logic has an edge case when files for higher versions don't have higher
              // timestamps, so we set the timestamp to scale with version rather than just being 0.
              fs.setTimes(logFilePath, version * 1000, 0)
            }
          }
        }
        deltaLog.cleanUpExpiredLogs()
        stream.processAllAvailable()

        val lastOffset = DeltaSourceOffset(
          deltaLog.tableId,
          SerializedOffset(stream.lastProgress.sources.head.endOffset))

        assert(lastOffset == DeltaSourceOffset(1, deltaLog.tableId, 3, -1, false))
      } finally {
        stream.stop()
      }
    }
  }

  test("Rate limited Delta source advances with non-data inserts") {
    withTempDirOnOCIs { (inputDir, outputDir, checkpointDir) =>
      Seq(1L, 2L, 3L).toDF("x").write.format("delta").save(inputDir.toString)

      val df = spark.readStream.format("delta").load(inputDir.toString)
      val stream = df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpointDir.toString)
        .option("maxFilesPerTrigger", 2)
        .start(outputDir.toString)
      try {
        val deltaLog = DeltaLog.forTable(spark, inputDir.toString)
        for(i <- 1 to 3) {
          deltaLog.startTransaction().commit(Seq(), DeltaOperations.ManualUpdate)
        }

        val fs = deltaLog.dataPath.getFileSystem(spark.sessionState.newHadoopConf())
        for (version <- 0 to 3) {
          val possibleFiles = Seq(
            f"/$version%020d.checkpoint.parquet",
            f"/$version%020d.json",
            f"/$version%020d.crc"
          ).map { name => new Path(inputDir.toString + "/_delta_log" + name) }
          for (logFilePath <- possibleFiles) {
            if (fs.exists(logFilePath)) {
              // The cleanup logic has an edge case when files for higher versions don't have higher
              // timestamps, so we set the timestamp to scale with version rather than just being 0.
              fs.setTimes(logFilePath, version * 1000, 0)
            }
          }
        }
        deltaLog.cleanUpExpiredLogs()
        stream.processAllAvailable()

        val lastOffset = DeltaSourceOffset(
          deltaLog.tableId,
          SerializedOffset(stream.lastProgress.sources.head.endOffset))

        assert(lastOffset == DeltaSourceOffset(1, deltaLog.tableId, 3, -1, false))
      } finally {
        stream.stop()
      }
    }
  }

  testQuietly("Delta sources should verify the protocol reader version") {
    withTempDirOnOCI { tempDir =>
      spark.range(0).write.format("delta").save(tempDir.getCanonicalPath)

      val df = spark.readStream.format("delta").load(tempDir.getCanonicalPath)
      val stream = df.writeStream
        .format("console")
        .start()
      try {
        stream.processAllAvailable()

        val deltaLog = DeltaLog.forTable(spark, tempDir.path)
        deltaLog.store.write(
          FileNames.deltaFile(deltaLog.logPath, deltaLog.snapshot.version + 1),
          // Write a large reader version to fail the streaming query
          Iterator(Protocol(minReaderVersion = Int.MaxValue).json))

        // The streaming query should fail because its version is too old
        val e = intercept[StreamingQueryException] {
          stream.processAllAvailable()
        }
        assert(e.getCause.isInstanceOf[InvalidProtocolVersionException])
      } finally {
        stream.stop()
      }
    }
  }
}

/**
 * A FileSystem implementation that returns monotonically increasing timestamps for file creation.
 * Note that we may return a different timestamp for the same file. This is okay for the tests
 * where we use this though.
 */
class MonotonicallyIncreasingTimestampFS extends RawLocalFileSystem {
  private var time: Long = System.currentTimeMillis()

  override def getScheme: String = MonotonicallyIncreasingTimestampFS.scheme

  override def getUri: URI = {
    URI.create(s"$getScheme:///")
  }

  override def getFileStatus(f: Path): FileStatus = {
    val original = super.getFileStatus(f)
    time += 1000L
    new FileStatus(original.getLen, original.isDirectory, 0, 0, time, f)
  }
}

object MonotonicallyIncreasingTimestampFS {
  val scheme = s"MonotonicallyIncreasingTimestampFS"
}

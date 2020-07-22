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

import java.io.{File, IOException}
import java.net.URI

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.delta.DeltaTestUtils.OptimisticTxnTestHelper
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.storage._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, FakeFileSystem, TrackingRenameFileSystem}
import org.apache.spark.sql.oci.{FileOnOCI, QueryTestOnOCI}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

abstract class LogStoreSuiteBaseOnOCI extends QueryTestOnOCI
  with LogStoreProvider
  with SharedSparkSession {

  def logStoreClassName: String

  protected override def sparkConf = {
    super.sparkConf.set(logStoreClassConfKey, logStoreClassName)
  }

  protected override val verifyLogStoreOnOCI: Boolean = false

  test("instantiation through SparkConf") {
    assert(spark.sparkContext.getConf.get(logStoreClassConfKey) == logStoreClassName)
    assert(LogStore(spark.sparkContext).getClass.getName == logStoreClassName)
  }

  test("read / write") {
    def assertNoLeakedCrcFiles(dir: FileOnOCI): Unit = {
      // crc file should not be leaked when origin file doesn't exist.
      // The implementation of Hadoop filesystem may filter out checksum file, so
      // listing files from local filesystem.
      val fileNames = listFilesOnOCI(dir).toSeq.filter(p => isFileOnOCI(p)).map(p => p.getName)
      val crcFiles = fileNames.filter(n => n.startsWith(".") && n.endsWith(".crc"))
      val originFileNamesForExistingCrcFiles = crcFiles.map { name =>
        // remove first "." and last ".crc"
        name.substring(1, name.length - 4)
      }

      // Check all origin files exist for all crc files.
      assert(originFileNamesForExistingCrcFiles.toSet.subsetOf(fileNames.toSet),
        s"Some of origin files for crc files don't exist - crc files: $crcFiles / " +
          s"expected origin files: $originFileNamesForExistingCrcFiles / actual files: $fileNames")
    }

    val tempDir = createTempDirOnOCI()
    try {
      val store = createLogStore(spark)

      val deltas = Seq(0, 1).map(i => new FileOnOCI(tempDir, i.toString)).map(_.getCanonicalPath)
      store.write(deltas.head, Iterator("zero", "none"))
      store.write(deltas(1), Iterator("one"))

      assert(store.read(deltas.head) == Seq("zero", "none"))
      assert(store.read(deltas(1)) == Seq("one"))

      assertNoLeakedCrcFiles(tempDir)
    } finally {
      deleteRecursivelyOnOCI(tempDir)
    }
  }

  test("detects conflict") {
    val tempDir = createTempDirOnOCI()
    try {
      val store = createLogStore(spark)

      val deltas = Seq(0, 1).map(i => new FileOnOCI(tempDir, i.toString)).map(_.getCanonicalPath)
      store.write(deltas.head, Iterator("zero"))
      store.write(deltas(1), Iterator("one"))

      intercept[java.nio.file.FileAlreadyExistsException] {
        store.write(deltas(1), Iterator("uno"))
      }
    } finally {
      deleteRecursivelyOnOCI(tempDir)
    }
  }

  test("listFrom") {
    val tempDir = createTempDirOnOCI()
    try {
      val store = createLogStore(spark)

      val deltas =
        Seq(0, 1, 2, 3, 4).map(i => new FileOnOCI(tempDir, i.toString)).map(_.toString).
          map(new Path(_))
      store.write(deltas(1), Iterator("zero"))
      store.write(deltas(2), Iterator("one"))
      store.write(deltas(3), Iterator("two"))

      assert(
        store.listFrom(deltas.head).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
      assert(
        store.listFrom(deltas(1)).map(_.getPath.getName).toArray === Seq(1, 2, 3).map(_.toString))
      assert(store.listFrom(deltas(2)).map(_.getPath.getName).toArray === Seq(2, 3).map(_.toString))
      assert(store.listFrom(deltas(3)).map(_.getPath.getName).toArray === Seq(3).map(_.toString))
      assert(store.listFrom(deltas(4)).map(_.getPath.getName).toArray === Nil)
    } finally {
      deleteRecursivelyOnOCI(tempDir)
    }
  }

  test("simple log store test") {
    val tempDir = createTempDirOnOCI()
    try {
      val log1 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      assert(log1.store.getClass.getName == logStoreClassName)

      val txn = log1.startTransaction()
      val file = AddFile("1", Map.empty, 1, 1, true) :: Nil
      txn.commitManually(file: _*)
      log1.checkpoint()

      DeltaLog.clearCache()
      val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
      assert(log2.store.getClass.getName == logStoreClassName)

      assert(log2.lastCheckpoint.map(_.version) === Some(0L))
      assert(log2.snapshot.allFiles.count == 1)
    } finally {
      deleteRecursivelyOnOCI(tempDir)
    }
  }

  protected def testHadoopConf(expectedErrMsg: String, fsImplConfs: (String, String)*): Unit = {
    test("should pick up fs impl conf from session Hadoop configuration") {
      withTempDir { tempDir =>
        val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))

        // Make sure it will fail without FakeFileSystem
        val e = intercept[IOException] {
          createLogStore(spark).listFrom(path)
        }
        assert(e.getMessage.contains(expectedErrMsg))
        withSQLConf(fsImplConfs: _*) {
          createLogStore(spark).listFrom(path)
        }
      }
    }
  }

  /**
   * Whether the log store being tested should use rename to write checkpoint or not. The following
   * test is using this method to verify the behavior of `checkpoint`.
   */
  protected def shouldUseRenameToWriteCheckpoint: Boolean

  // Not Using Object Store since this test is based on Java File API
  test("use isPartialWriteVisible to decide whether use rename") {
    withTempDir { tempDir =>
      import testImplicits._
      Seq(1, 2, 4).toDF().write.format("delta").save(tempDir.getCanonicalPath)
      withSQLConf(
          "fs.file.impl" -> classOf[TrackingRenameFileSystem].getName,
          "fs.file.impl.disable.cache" -> "true") {
        val logStore = DeltaLog.forTable(spark, tempDir.getCanonicalPath)
        TrackingRenameFileSystem.numOfRename = 0
        logStore.checkpoint()
        val expectedNumOfRename = if (shouldUseRenameToWriteCheckpoint) 1 else 0
        assert(TrackingRenameFileSystem.numOfRename === expectedNumOfRename)
      }
    }
  }
}

class AzureLogStoreSuiteOnOCI extends LogStoreSuiteBaseOnOCI {

  override val logStoreClassName: String = classOf[AzureLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class HDFSLogStoreSuiteOnOCI extends LogStoreSuiteBaseOnOCI {

  override val logStoreClassName: String = classOf[HDFSLogStore].getName
  // HDFSLogStore is based on FileContext APIs and hence requires AbstractFileSystem-based
  // implementations.
  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  import testImplicits._

  test("writes on systems without AbstractFileSystem implemented") {
    withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
      "fs.fake.impl.disable.cache" -> "true") {
      val tempDir = Utils.createTempDir()
      val path = new Path(new URI(s"fake://${tempDir.toURI.getRawPath}/1.json"))
      val e = intercept[IOException] {
        createLogStore(spark).write(path, Iterator("zero", "none"))
      }
      assert(e.getMessage.contains(
        DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null).getMessage))
    }
  }

  test("reads should work on systems without AbstractFileSystem implemented") {
    withTempDir { tempDir =>
      val writtenFile = new File(tempDir, "1")
      val store = createLogStore(spark)
      store.write(writtenFile.getCanonicalPath, Iterator("zero", "none"))
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val read = createLogStore(spark).read("fake://" + writtenFile.getCanonicalPath)
        assert(read === ArrayBuffer("zero", "none"))
      }
    }
  }

  test("No AbstractFileSystem - end to end test using data frame") {
    // Writes to the fake file system will fail
    withTempDir { tempDir =>
      val fakeFSLocation = s"fake://${tempDir.getCanonicalFile}"
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val e = intercept[IOException] {
          Seq(1, 2, 4).toDF().write.format("delta").save(fakeFSLocation)
        }
        assert(e.getMessage.contains(
          DeltaErrors.incorrectLogStoreImplementationException(sparkConf, null).getMessage))
      }
    }
    // Reading files written by other systems will work.
    withTempDir { tempDir =>
      Seq(1, 2, 4).toDF().write.format("delta").save(tempDir.getAbsolutePath)
      withSQLConf("fs.fake.impl" -> classOf[FakeFileSystem].getName,
        "fs.fake.impl.disable.cache" -> "true") {
        val fakeFSLocation = s"fake://${tempDir.getCanonicalFile}"
        checkAnswer(spark.read.format("delta").load(fakeFSLocation), Seq(1, 2, 4).toDF())
      }
    }
  }

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class LocalLogStoreSuiteOnOCI extends LogStoreSuiteBaseOnOCI {

  override val logStoreClassName: String = classOf[LocalLogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}

class OCILogStoreSuiteOnOCI extends LogStoreSuiteBaseOnOCI {

  override val logStoreClassName: String = classOf[OCILogStore].getName

  testHadoopConf(
    expectedErrMsg = "No FileSystem for scheme: fake",
    "fs.fake.impl" -> classOf[FakeFileSystem].getName,
    "fs.fake.impl.disable.cache" -> "true")

  protected def shouldUseRenameToWriteCheckpoint: Boolean = true
}


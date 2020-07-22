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

package org.apache.spark.sql.oci

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.util.UUID

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}

import scala.io.Source.createBufferedSource
import scala.io.{BufferedSource, Codec, Source}

trait TestUtilsOnOCI extends Logging with Eventually {

  protected val verifyLogStoreOnOCI: Boolean = true

  protected def verifySparkConf(conf: SparkConf): Unit = {
    if (verifyLogStoreOnOCI) {
      assert(conf.get("spark.delta.logStore.class") ==
        "org.apache.spark.sql.delta.storage.OCILogStore")
    }
  }

  protected def getRunTest(conf: SparkConf): Boolean = {
    // BucketName and Namespace should be Published in Documentation
    // For others, refer @see https://docs.cloud.oracle.com/en-us/iaas/tools/hdfs/2.9.2.1/
    conf.contains("spark.hadoop.fs.oci.client.bucketname") &&
      conf.contains("spark.hadoop.fs.oci.client.namespace") &&
      conf.contains("spark.hadoop.fs.oci.client.hostname") &&
      conf.contains("spark.hadoop.fs.oci.client.auth.pemfilepath") &&
      conf.contains("spark.hadoop.fs.oci.client.auth.tenantId") &&
      conf.contains("spark.hadoop.fs.oci.client.auth.userId") &&
      conf.contains("spark.hadoop.fs.oci.client.auth.fingerprint") &&
      conf.contains("spark.hadoop.fs.AbstractFileSystem.oci.impl") &&
      conf.get("spark.hadoop.fs.AbstractFileSystem.oci.impl") == "com.oracle.bmc.hdfs.Bmc"
  }

  protected def setLogStoreClass(conf: SparkConf): SparkConf = {
    conf.set("spark.delta.logStore.class",
      "org.apache.spark.sql.delta.storage.OCILogStore")
  }

  def fs(implicit spark: SparkSession, file: FileOnOCI): FileSystem = {
    val conf = spark.sessionState.newHadoopConf
    file.path.getFileSystem(conf)
  }

  def ociSchemaAndAuthority()(implicit spark: SparkSession): String = {
    val conf: SparkConf = spark.sparkContext.getConf
    // @see https://docs.cloud.oracle.com/en-us/iaas/tools/hdfs/2.9.2.1/
    val bucket = conf.get("spark.hadoop.fs.oci.client.bucketname")
    val namespace = conf.get("spark.hadoop.fs.oci.client.namespace")
    val authority = s"$bucket@$namespace"
    s"oci://$authority/"
  }

  def createTempDirOnOCI(tmpdir: String = System.getProperty("java.io.tmpdir", "tmp"),
                         namePrefix: String = "spark")
                        (implicit spark: SparkSession): FileOnOCI = {
    val root: Path = new Path(ociSchemaAndAuthority)
    val dir = new File(tmpdir,
      namePrefix + "-" + getClass.getSimpleName + "-" + UUID.randomUUID.toString)
    val ociFile = new FileOnOCI(new FileOnOCI(root), dir.getCanonicalPath)
    assert(mkdirOnOCI(ociFile))
    ociFile
  }

  private def withTempDir(f: FileOnOCI => Unit)
                         (implicit spark: SparkSession): Unit = {
    val dir = createTempDirOnOCI()
    try f(dir) finally {
      deleteRecursivelyOnOCI(dir)
    }
  }

  def withTempDirOnOCI(f: FileOnOCI => Unit)
                      (implicit spark: SparkSession): Unit = {
    withTempDir { dir =>
      f(dir)
      waitForTasksToFinish()
    }
  }

  /**
   * Waits for all tasks on all executors to be finished.
   */
  private def waitForTasksToFinish()(implicit spark: SparkSession): Unit = {
    eventually(timeout(Span(10, Seconds))) {
      assert(spark.sparkContext.statusTracker
        .getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  @throws[IOException]
  def mkdirOnOCI(file: FileOnOCI)(implicit spark: SparkSession): Boolean =
    fs(spark, file).mkdirs(file.path)

  @throws[IOException]
  def deleteOnOCI(file: FileOnOCI, recursive: Boolean = false)
                 (implicit spark: SparkSession): Boolean =
    fs(spark, file).delete(file.path, recursive)

  @throws[IOException]
  def deleteRecursivelyOnOCI(file: FileOnOCI, recursive: Boolean = false)
                 (implicit spark: SparkSession): Boolean = deleteOnOCI(file, true)

  @throws[IOException]
  def existsOnOCI(file: FileOnOCI)(implicit spark: SparkSession): Boolean =
    fs(spark, file).exists(file.path)

  def setLastModifiedOnOCI(file: FileOnOCI, time: Long): Boolean = {
    // Not supported @see setTimes
    true
  }

  @throws[IOException]
  def listFilesOnOCI(file: FileOnOCI)
                    (implicit spark: SparkSession): Array[FileOnOCI] = {
    var status: Array[FileStatus] = new Array[FileStatus](0)
    status = fs(spark, file).listStatus(file.path)
    status.filter(_.isFile).map(f => new FileOnOCI(f.getPath))
  }

  @throws[IOException]
  def isFileOnOCI(file: FileOnOCI)(implicit spark: SparkSession): Boolean =
    fs(spark, file).isFile(file.path)

  def renameToOnOCI(from: FileOnOCI, to: FileOnOCI)
                   (implicit spark: SparkSession): Boolean =
    fs(spark, from).rename(from.path, to.path)

  @throws[IOException]
  def writeOnOCI(file: FileOnOCI, data: CharSequence)(implicit spark: SparkSession): Unit = {
    val str = if (data == null) null else data.toString
    var out: FSDataOutputStream = null
    try {
      out = fs(spark, file).create(file.path)
      IOUtils.write(str, out, Charset.defaultCharset)
      out.close() // don't swallow close Exception if copy completes normally
    } finally IOUtils.closeQuietly(out)
  }

  def fromFileOnOCI(file: FileOnOCI)
                   (implicit spark: SparkSession, codec: Codec): BufferedSource = {
    fromFile(file, Source.DefaultBufSize)(spark, codec)
  }

  private def fromFile(file: FileOnOCI, bufferSize: Int)
                      (implicit spark: SparkSession, codec: Codec): BufferedSource = {
    val inputStream: FSDataInputStream = openInputStreamOnOCI(file)
    createBufferedSource(
      inputStream,
      bufferSize,
      () => fromFile(file, bufferSize)(spark, codec),
      () => inputStream.close()
    )(codec) withDescription ("file:" + file.getAbsolutePath)
  }

  @throws[IOException]
  def openInputStreamOnOCI(file: FileOnOCI)(implicit spark: SparkSession): FSDataInputStream =
    fs(spark, file).open(file.path)

  // scalastyle:off println
  def logConsole(line: String): Unit = println(line)
  // scalastyle:on println

  protected def testOrIgnore(conf: SparkConf, testName: String, test: => Unit,
                             ignore: => Unit): Unit = {
    if (getRunTest(conf)) {
      test
    } else {
      ignore
      val msg = testIgnoreMessage(s"${getClass.getSimpleName}::$testName")
      logConsole(msg)
      logInfo(msg)
    }
  }

  def testIgnoreMessage(testName: String): String =
    s"Test ignored because OCI properties are not set: $testName"
}

class JavaTestUtilsOnOCI extends TestUtilsOnOCI

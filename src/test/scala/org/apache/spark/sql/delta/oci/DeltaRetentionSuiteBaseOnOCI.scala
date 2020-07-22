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

import org.apache.spark.sql.delta.DeltaOperations.Truncate
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, OptimisticTransaction}
import org.apache.spark.sql.oci.{FileOnOCI, QueryTestOnOCI}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.unsafe.types.UTF8String

trait DeltaRetentionSuiteBaseOnOCI extends QueryTestOnOCI
  with SharedSparkSession {
  protected val testOp = Truncate()

  protected override def sparkConf: SparkConf = super.sparkConf
    // Disable the log cleanup because it runs asynchronously and causes test flakiness
    .set("spark.databricks.delta.properties.defaults.enableExpiredLogCleanup", "false")

  protected def intervalStringToMillis(str: String): Long = {
    DeltaConfigs.getMilliSeconds(
      IntervalUtils.safeStringToInterval(UTF8String.fromString(str)))
  }

  protected def getDeltaFiles(dir: FileOnOCI): Seq[FileOnOCI] =
    listFilesOnOCI(dir).filter(_.getName.endsWith(".json"))

  protected def getCheckpointFiles(dir: FileOnOCI): Seq[FileOnOCI] =
    listFilesOnOCI(dir).filter(f => FileNames.isCheckpointFile(new Path(f.getCanonicalPath)))

  protected def getLogFiles(dir: FileOnOCI): Seq[FileOnOCI]

  /**
   * Start a txn that disables automatic log cleanup. Some tests may need to manually clean up logs
   * to get deterministic behaviors.
   */
  protected def startTxnWithManualLogCleanup(log: DeltaLog): OptimisticTransaction = {
    val txn = log.startTransaction()
    // This will pick up `spark.databricks.delta.properties.defaults.enableExpiredLogCleanup` to
    // disable log cleanup.
    txn.updateMetadata(Metadata())
    txn
  }

  test("startTxnWithManualLogCleanup") {
    withTempDirOnOCI { tempDir =>
      val log = DeltaLog(spark, new Path(tempDir.getCanonicalPath))
      startTxnWithManualLogCleanup(log).commit(Nil, testOp)
      assert(!log.enableExpiredLogCleanup)
    }
  }
}

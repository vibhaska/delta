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

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalactic.source
import org.scalatest.Tag

trait StreamTestOnOCI extends StreamTest with SharedSparkSession with TestUtilsOnOCI {

  protected override def sparkConf: SparkConf = setLogStoreClass(super.sparkConf)

  override def beforeEach() {
    super.beforeEach()
    verifySparkConf(spark.sparkContext.getConf)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any /* Assertion */)
                             (implicit pos: source.Position): Unit = {
    testOrIgnore(super.sparkConf, testName, super.test(testName, testTags: _*)(testFun)(pos),
      super.ignore(testName, testTags: _*)(testFun)(pos))
  }
}

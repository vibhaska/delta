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

package io.delta.oci.sql;

import org.apache.spark.sql.oci.FileOnOCI;
import org.apache.spark.sql.oci.JavaTestUtilsOnOCI;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.io.IOException;

public class JavaDeltaSparkSessionExtensionSuiteOnOCI {

    @Test
    public void testSQLConf() throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("JavaDeltaSparkSessionExtensionSuiteUsingSQLConf")
                .master("local[2]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .getOrCreate();
        JavaTestUtilsOnOCI javaTestUtils = new JavaTestUtilsOnOCI();
        javaTestUtils.setLogStoreClass(spark.sparkContext().getConf());
        Boolean runTests = javaTestUtils.getRunTest(spark.sparkContext().getConf());
        FileOnOCI input = null;
        try {
            if (runTests) {
                input = javaTestUtils.createTempDirOnOCI(
                        System.getProperty("java.io.tmpdir"), "spark", spark);
                spark.range(1, 10).write().format("delta").save(input.getCanonicalPath());
                spark.sql("vacuum delta.`" + input + "`");
            } else {
                javaTestUtils.logConsole(ignoreTestSQLConf());
                spark.logInfo(this::ignoreTestSQLConf);
            }
        } finally {
            if (input != null) {
                javaTestUtils.deleteOnOCI(input, true, spark);
            }
            spark.stop();
        }
    }

    private String ignoreTestSQLConf() {
        JavaTestUtilsOnOCI javaTestUtils = new JavaTestUtilsOnOCI();
        return javaTestUtils.testIgnoreMessage("JavaDeltaSparkSessionExtensionSuiteOnOCI::testSQLConf");
    }
}

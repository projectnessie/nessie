/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.nessie.deltalake;

import com.dremio.nessie.server.NessieTestServerBinder;
import io.delta.tables.DeltaTable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.QueryTest$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestOperations {
//  private static TestNessieServer server;
  private transient TestSparkSession spark;
  private transient String input;

  @BeforeAll
  public static void create() throws Exception {
    NessieTestServerBinder.settings.setDefaultTag("master");
//    server = new TestNessieServer();
//    server.start(9898);
  }

  @BeforeEach
  public void setUp() {
    // Trigger static initializer of TestData
    SparkConf conf = new SparkConf();
    conf.set("spark.delta.logStore.class", "com.dremio.nessie.deltalake.DeltaLake");
    spark = new TestSparkSession(conf);
    spark.sparkContext().hadoopConfiguration().set("nessie.url", "http://localhost:19120/api/v1");
    spark.sparkContext().hadoopConfiguration().set("nessie.username", "admin_user");
    spark.sparkContext().hadoopConfiguration().set("nessie.password", "test123");
    spark.sparkContext().hadoopConfiguration().set("nessie.auth.type", "basic");
  }

  @AfterEach
  public void tearDown() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void testAPI() {
    String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input").toString();
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
    Assertions.assertTrue(DeltaTable.isDeltaTable(input));
  }
}

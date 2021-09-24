/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.deltalake;

import io.delta.tables.DeltaTable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.tests.AbstractSparkTest;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import scala.Tuple2;

class ITDeltaLog extends AbstractSparkTest {

  NessieApiV1 api;

  @TempDir File tempPath;

  @BeforeAll
  protected static void createDelta() {
    conf.set("spark.delta.logStore.class", NessieLogStore.class.getCanonicalName())
        .set("spark.delta.logFileHandler.class", NessieLogFileMetaParser.class.getCanonicalName())
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
  }

  @BeforeEach
  void createClient() {
    api = HttpClientBuilder.builder().withUri(url).build(NessieApiV1.class);
  }

  @AfterEach
  void closeClient() {
    try {
      if (api != null) {
        api.close();
      }
    } finally {
      api = null;
    }
  }

  @Test
  // Delta < 0.8 w/ Spark 2.x doesn't support multiple branches well (warnings when changing the
  // configuration)
  @DisabledIfSystemProperty(named = "skip-multi-branch-tests", matches = "true")
  void testMultipleBranches() throws Exception {
    String csvSalaries1 = ITDeltaLog.class.getResource("/salaries1.csv").getPath();
    String csvSalaries2 = ITDeltaLog.class.getResource("/salaries2.csv").getPath();
    String csvSalaries3 = ITDeltaLog.class.getResource("/salaries3.csv").getPath();
    String pathSalaries = new File(tempPath, "salaries").getAbsolutePath();

    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS test_multiple_branches (Season STRING, Team STRING, Salary STRING, "
                + "Player STRING) USING delta LOCATION '%s'",
            pathSalaries));
    Dataset<Row> salariesDf1 = spark.read().option("header", true).csv(csvSalaries1);
    salariesDf1.write().format("delta").mode("overwrite").save(pathSalaries);

    Dataset<Row> count1 = spark.sql("SELECT COUNT(*) FROM test_multiple_branches");
    Assertions.assertEquals(15L, count1.collectAsList().get(0).getLong(0));

    Reference mainBranch = api.getReference().refName("main").get();

    Reference devBranch =
        api.createReference()
            .sourceRefName(mainBranch.getName())
            .reference(Branch.of("testMultipleBranches", mainBranch.getHash()))
            .create();

    spark.sparkContext().conf().set("spark.sql.catalog.spark_catalog.ref", devBranch.getName());

    Dataset<Row> salariesDf2 = spark.read().option("header", true).csv(csvSalaries2);
    salariesDf2.write().format("delta").mode("append").save(pathSalaries);

    Dataset<Row> count2 = spark.sql("SELECT COUNT(*) FROM test_multiple_branches");
    Assertions.assertEquals(30L, count2.collectAsList().get(0).getLong(0));

    spark.sparkContext().conf().set("spark.sql.catalog.spark_catalog.ref", "main");

    Dataset<Row> salariesDf3 = spark.read().option("header", true).csv(csvSalaries3);
    salariesDf3.write().format("delta").mode("append").save(pathSalaries);

    Dataset<Row> count3 = spark.sql("SELECT COUNT(*) FROM test_multiple_branches");
    Assertions.assertEquals(35L, count3.collectAsList().get(0).getLong(0));
  }

  @Test
  // Delta < 0.8 w/ Spark 2.x doesn't support multiple branches well (warnings when changing the
  // configuration)
  @DisabledIfSystemProperty(named = "skip-multi-branch-tests", matches = "true")
  void testCommitRetry() throws Exception {
    String csvSalaries1 = ITDeltaLog.class.getResource("/salaries1.csv").getPath();
    String csvSalaries2 = ITDeltaLog.class.getResource("/salaries2.csv").getPath();
    String csvSalaries3 = ITDeltaLog.class.getResource("/salaries3.csv").getPath();
    String pathSalaries = new File(tempPath, "salaries").getAbsolutePath();

    spark.sql(
        String.format(
            "CREATE TABLE IF NOT EXISTS test_commit_retry (Season STRING, Team STRING, Salary STRING, "
                + "Player STRING) USING delta LOCATION '%s'",
            pathSalaries));
    Dataset<Row> salariesDf1 = spark.read().option("header", true).csv(csvSalaries1);
    salariesDf1.write().format("delta").mode("overwrite").save(pathSalaries);

    Dataset<Row> count1 = spark.sql("SELECT COUNT(*) FROM test_commit_retry");
    Assertions.assertEquals(15L, count1.collectAsList().get(0).getLong(0));

    Reference mainBranch = api.getReference().refName("main").get();

    Reference devBranch =
        api.createReference()
            .sourceRefName(mainBranch.getName())
            .reference(Branch.of("testCommitRetry", mainBranch.getHash()))
            .create();

    spark.sparkContext().conf().set("spark.sql.catalog.spark_catalog.ref", devBranch.getName());

    Dataset<Row> salariesDf2 = spark.read().option("header", true).csv(csvSalaries2);
    salariesDf2.write().format("delta").mode("append").save(pathSalaries);

    Dataset<Row> count2 = spark.sql("SELECT COUNT(*) FROM test_commit_retry");
    Assertions.assertEquals(30L, count2.collectAsList().get(0).getLong(0));

    Reference to = api.getReference().refName("main").get();
    Reference from = api.getReference().refName("testCommitRetry").get();

    api.mergeRefIntoBranch().branch((Branch) to).fromRef(from).merge();

    spark.sparkContext().conf().set("spark.sql.catalog.spark_catalog.ref", "main");

    Dataset<Row> salariesDf3 = spark.read().option("header", true).csv(csvSalaries3);
    salariesDf3.write().format("delta").mode("append").save(pathSalaries);

    Dataset<Row> count3 = spark.sql("SELECT COUNT(*) FROM test_commit_retry");
    Assertions.assertEquals(50L, count3.collectAsList().get(0).getLong(0));
  }

  @Test
  void testWithoutCondition() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    target.delete();

    List<Object[]> expectedAnswer = new ArrayList<>();
    assertEquals("testWithoutCondition", expectedAnswer, transform(target.toDF()));
  }

  @Test
  public void testWithCondition() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    target.delete("key = 1 or key = 2");

    Dataset<Row> expectedAnswer = createKVDataSet(Arrays.asList(tuple2(3, 30), tuple2(4, 40)));
    assertEquals("testWithCondition", transform(target.toDF()), transform(expectedAnswer));
  }

  @Test
  public void testWithColumnCondition() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    target.delete(functions.expr("key = 1 or key = 2"));

    Dataset<Row> expectedAnswer = createKVDataSet(Arrays.asList(tuple2(3, 30), tuple2(4, 40)));
    assertEquals("testWithColumnCondition", transform(target.toDF()), transform(expectedAnswer));
  }

  private Dataset<Row> createKVDataSet(
      List<Tuple2<Integer, Integer>> data, String keyName, String valueName) {
    Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
    return spark.createDataset(data, encoder).toDF(keyName, valueName);
  }

  private Dataset<Row> createKVDataSet(List<Tuple2<Integer, Integer>> data) {
    Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
    return spark.createDataset(data, encoder).toDF();
  }

  private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }

  @Test
  public void testWithoutCondition2() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    Map<String, String> set =
        new HashMap<String, String>() {
          {
            put("key", "100");
          }
        };
    target.updateExpr(set);

    Dataset<Row> expectedAnswer =
        createKVDataSet(
            Arrays.asList(tuple2(100, 10), tuple2(100, 20), tuple2(100, 30), tuple2(100, 40)));
    assertEquals(
        "testWithoutCondition2",
        transform(target.toDF().sort("key", "value")),
        transform(expectedAnswer));
  }

  @Test
  public void testWithoutConditionUsingColumn() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    Map<String, Column> set =
        new HashMap<String, Column>() {
          {
            put("key", functions.expr("100"));
          }
        };
    target.update(set);

    Dataset<Row> expectedAnswer =
        createKVDataSet(
            Arrays.asList(tuple2(100, 10), tuple2(100, 20), tuple2(100, 30), tuple2(100, 40)));
    assertEquals(
        "testWithoutConditionUsingColumn",
        transform(target.toDF().sort("value")),
        transform(expectedAnswer));
  }

  @Test
  public void testWithCondition2() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    Map<String, String> set =
        new HashMap<String, String>() {
          {
            put("key", "100");
          }
        };
    target.updateExpr("key = 1 or key = 2", set);

    Dataset<Row> expectedAnswer =
        createKVDataSet(
            Arrays.asList(tuple2(100, 10), tuple2(100, 20), tuple2(3, 30), tuple2(4, 40)));
    assertEquals("testWithCondition2", transform(target.toDF()), transform(expectedAnswer));
  }

  @Test
  public void testWithConditionUsingColumn() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());

    Map<String, Column> set =
        new HashMap<String, Column>() {
          {
            put("key", functions.expr("100"));
          }
        };
    target.update(functions.expr("key = 1 or key = 2"), set);

    Dataset<Row> expectedAnswer =
        createKVDataSet(
            Arrays.asList(tuple2(100, 10), tuple2(100, 20), tuple2(3, 30), tuple2(4, 40)));
    assertEquals(
        "testWithConditionUsingColumn", transform(target.toDF()), transform(expectedAnswer));
  }

  @Test
  public void checkBasicApi() {
    Dataset<Row> targetTable =
        createKVDataSet(Arrays.asList(tuple2(1, 10), tuple2(2, 20)), "key1", "value1");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());

    Dataset<Row> sourceTable =
        createKVDataSet(Arrays.asList(tuple2(1, 100), tuple2(3, 30)), "key2", "value2");

    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());
    Map<String, String> updateMap =
        new HashMap<String, String>() {
          {
            put("key1", "key2");
            put("value1", "value2");
          }
        };
    Map<String, String> insertMap =
        new HashMap<String, String>() {
          {
            put("key1", "key2");
            put("value1", "value2");
          }
        };
    target
        .merge(sourceTable, "key1 = key2")
        .whenMatched()
        .updateExpr(updateMap)
        .whenNotMatched()
        .insertExpr(insertMap)
        .execute();

    Dataset<Row> expectedAnswer =
        createKVDataSet(Arrays.asList(tuple2(1, 100), tuple2(2, 20), tuple2(3, 30)));

    assertEquals("checkBasicApi", transform(target.toDF().sort("key1")), transform(expectedAnswer));
  }

  @Test
  public void checkExtendedApi() {
    Dataset<Row> targetTable =
        createKVDataSet(Arrays.asList(tuple2(1, 10), tuple2(2, 20)), "key1", "value1");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());

    Dataset<Row> sourceTable =
        createKVDataSet(Arrays.asList(tuple2(1, 100), tuple2(3, 30)), "key2", "value2");

    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());
    Map<String, String> updateMap =
        new HashMap<String, String>() {
          {
            put("key1", "key2");
            put("value1", "value2");
          }
        };
    Map<String, String> insertMap =
        new HashMap<String, String>() {
          {
            put("key1", "key2");
            put("value1", "value2");
          }
        };
    target
        .merge(sourceTable, "key1 = key2")
        .whenMatched("key1 = 4")
        .delete()
        .whenMatched("key2 = 1")
        .updateExpr(updateMap)
        .whenNotMatched("key2 = 3")
        .insertExpr(insertMap)
        .execute();

    Dataset<Row> expectedAnswer =
        createKVDataSet(Arrays.asList(tuple2(1, 100), tuple2(2, 20), tuple2(3, 30)));
    assertEquals(
        "checkExtendedApiWithColumn",
        transform(target.toDF().sort("key1")),
        transform(expectedAnswer));
  }

  @Test
  public void checkExtendedApiWithColumn() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(4, 40)), "key1", "value1");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());

    Dataset<Row> sourceTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 100), tuple2(3, 30), tuple2(4, 41)), "key2", "value2");

    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());
    Map<String, Column> updateMap =
        new HashMap<String, Column>() {
          {
            put("key1", functions.col("key2"));
            put("value1", functions.col("value2"));
          }
        };
    Map<String, Column> insertMap =
        new HashMap<String, Column>() {
          {
            put("key1", functions.col("key2"));
            put("value1", functions.col("value2"));
          }
        };
    target
        .merge(sourceTable, functions.expr("key1 = key2"))
        .whenMatched(functions.expr("key1 = 4"))
        .delete()
        .whenMatched(functions.expr("key2 = 1"))
        .update(updateMap)
        .whenNotMatched(functions.expr("key2 = 3"))
        .insert(insertMap)
        .execute();

    Dataset<Row> expectedAnswer =
        createKVDataSet(Arrays.asList(tuple2(1, 100), tuple2(2, 20), tuple2(3, 30)));

    assertEquals(
        "checkExtendedApiWithColumn",
        transform(target.toDF().sort("key1")),
        transform(expectedAnswer));
  }

  @Test
  public void checkUpdateAllAndInsertAll() {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(4, 40), tuple2(5, 50)),
            "key",
            "value");
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());

    Dataset<Row> sourceTable =
        createKVDataSet(
            Arrays.asList(
                tuple2(1, 100), tuple2(3, 30), tuple2(4, 41), tuple2(5, 51), tuple2(6, 60)),
            "key",
            "value");

    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());
    target
        .as("t")
        .merge(sourceTable.as("s"), functions.expr("t.key = s.key"))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute();

    Dataset<Row> expectedAnswer =
        createKVDataSet(
            Arrays.asList(
                tuple2(1, 100),
                tuple2(2, 20),
                tuple2(3, 30),
                tuple2(4, 41),
                tuple2(5, 51),
                tuple2(6, 60)));

    assertEquals(
        "checkUpdateAllAndInsertAll",
        transform(target.toDF().sort("key")),
        transform(expectedAnswer));
  }

  @Test
  public void testAPI() {
    String input = Utils.createTempDir(System.getProperty("java.io.tmpdir"), "input").toString();
    List<String> data = Arrays.asList("hello", "world");
    Dataset<Row> dataDF = spark.createDataset(data, Encoders.STRING()).toDF();
    dataDF.write().format("delta").mode("overwrite").save(input);

    // Test creating DeltaTable by path
    DeltaTable table1 = DeltaTable.forPath(spark, input);
    assertEquals(
        "Test creating DeltaTable by path",
        transform(table1.toDF().sort("value")),
        transform(dataDF));

    // Test creating DeltaTable by path picks up active SparkSession
    DeltaTable table2 = DeltaTable.forPath(input);
    assertEquals(
        "Test creating DeltaTable by path picks up active SparkSession",
        transform(table2.toDF().sort("value")),
        transform(dataDF));

    // Test DeltaTable.as() creates subquery alias
    assertEquals(
        "Test DeltaTable.as() creates subquery alias",
        transform(table2.as("tbl").toDF().select("tbl.value").sort("tbl.value")),
        transform(dataDF));

    // Test DeltaTable.isDeltaTable() is true for a Delta file path.
    Assertions.assertTrue(DeltaTable.isDeltaTable(input));
  }
}

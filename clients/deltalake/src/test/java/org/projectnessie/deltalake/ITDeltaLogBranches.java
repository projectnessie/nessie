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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.tests.AbstractSparkTest;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.Reference;
import scala.Tuple2;

class ITDeltaLogBranches extends AbstractSparkTest {

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
  void closeClient() throws BaseNessieClientServerException {
    Reference ref = null;
    try {
      ref = api.getReference().refName("test").get();
    } catch (NessieNotFoundException e) {
      // pass ignore
    }
    if (ref != null) {
      api.deleteBranch().branch((Branch) ref).delete();
    }
    try {
      api.close();
    } finally {
      api = null;
    }
  }

  @Test
  void testBranches() throws BaseNessieClientServerException {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    // write some data to table
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    // create test at the point where there is only 1 commit
    Branch sourceRef = api.getDefaultBranch();
    api.createReference()
        .sourceRefName(sourceRef.getName())
        .reference(Branch.of("test", sourceRef.getHash()))
        .create();
    // add some more data to main
    targetTable.write().format("delta").mode("append").save(tempPath.getAbsolutePath());

    // read main and record number of rows
    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());
    int expectedSize = target.toDF().collectAsList().size();

    /*
    It is hard to change ref in Detla for the following reasons
    * DeltaTables are cached
    * hadoop/spark config don't get updated in the cached tables
    * there is currently no way to pass down a branch or hash via '@' or '#'
    Below we manually invaildate the cache and update the ref before reading the table off test
    As the table itself is cached we can't read from main w/o invalidating, hence reading from main above
     */
    DeltaLog.invalidateCache(spark, new Path(tempPath.getAbsolutePath()));
    spark.sparkContext().conf().set("spark.sql.catalog.spark_catalog.ref", "test");
    Dataset<Row> targetBranch = spark.read().format("delta").load(tempPath.getAbsolutePath());

    // we expect the table from test to be half the size of the table from main
    Assertions.assertEquals(expectedSize * 0.5, targetBranch.collectAsList().size());
  }

  @Test
  void testCheckpoint() throws NessieNotFoundException {
    Dataset<Row> targetTable =
        createKVDataSet(
            Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)),
            "key",
            "value");
    // write some data to table
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    // write enough to trigger a checkpoint generation
    for (int i = 0; i < 15; i++) {
      targetTable.write().format("delta").mode("append").save(tempPath.getAbsolutePath());
    }

    DeltaTable target = DeltaTable.forPath(spark, tempPath.getAbsolutePath());
    int expectedSize = target.toDF().collectAsList().size();
    Assertions.assertEquals(64, expectedSize);

    String tableName = tempPath.getAbsolutePath() + "/_delta_log";
    ContentsKey key = ContentsKey.of(tableName.split("/"));
    Contents contents = api.getContents().key(key).refName("main").get().get(key);
    Optional<DeltaLakeTable> table = contents.unwrap(DeltaLakeTable.class);
    Assertions.assertTrue(table.isPresent());
    Assertions.assertEquals(1, table.get().getCheckpointLocationHistory().size());
    Assertions.assertEquals(5, table.get().getMetadataLocationHistory().size());
    Assertions.assertNotNull(table.get().getLastCheckpoint());
  }

  private Dataset<Row> createKVDataSet(
      List<Tuple2<Integer, Integer>> data, String keyName, String valueName) {
    Encoder<Tuple2<Integer, Integer>> encoder = Encoders.tuple(Encoders.INT(), Encoders.INT());
    return spark.createDataset(data, encoder).toDF(keyName, valueName);
  }

  private <T1, T2> Tuple2<T1, T2> tuple2(T1 t1, T2 t2) {
    return new Tuple2<>(t1, t2);
  }
}

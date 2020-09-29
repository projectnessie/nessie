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
package com.dremio.nessie.deltalake;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.functions;
import org.apache.spark.util.Utils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.tests.AbstractSparkTest;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;

import io.delta.tables.DeltaTable;
import scala.Tuple2;

class ITDeltaLogBranches extends AbstractSparkTest {

  private NessieClient client;

  @TempDir
  File tempPath;

  @BeforeAll
  protected static void createDelta() {
    conf.set("spark.delta.logStore.class", NessieLogStore.class.getCanonicalName())
        .set("spark.delta.logFileHandler.class", NessieLogFileMetaParser.class.getCanonicalName())
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
  }

  @BeforeEach
  public void createClient() {
    client = new NessieClient(AuthType.NONE, url, null, null);

  }

  @AfterEach
  public void closeClient() throws NessieNotFoundException, NessieConflictException {
    client.getTreeApi().deleteBranch("test", client.getTreeApi().getReferenceByName("test").getHash());
    client.close();
    client = null;
  }

  @Test
  void testBranches() throws NessieNotFoundException, NessieConflictException {
    Dataset<Row> targetTable = createKVDataSet(Arrays.asList(tuple2(1, 10), tuple2(2, 20), tuple2(3, 30), tuple2(4, 40)), "key", "value");
    // write some data to table
    targetTable.write().format("delta").save(tempPath.getAbsolutePath());
    // create test at the point where there is only 1 commit
    client.getTreeApi().createNewBranch("test", client.getTreeApi().getDefaultBranch().getHash());
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
    spark.sparkContext().hadoopConfiguration().set("nessie.ref", "test");
    Dataset<Row> targetBranch = spark.read().format("delta").load(tempPath.getAbsolutePath());

    // we expect the table from test to be half the size of the table from main
    Assertions.assertEquals(expectedSize * 0.5, targetBranch.collectAsList().size());

  }


  private Dataset<Row> createKVDataSet(List<Tuple2<Integer, Integer>> data, String keyName, String valueName) {
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


}

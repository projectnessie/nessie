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
package com.dremio.nessie.iceberg.spark;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.iceberg.NessieCatalog;
import com.google.common.collect.ImmutableList;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class ITIcebergSpark {

  private static final Object ANY = new Object();
  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema schema = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
                                                                      required(2, "foe2", Types.StringType.get())).fields());
  private static SparkSession spark;
  private static NessieCatalog catalog;
  private static File alleyLocalDir;
  private static Configuration hadoopConfig;
  private static String url;

  private Path tableLocation;

  @BeforeAll
  static void create() throws IOException {
    alleyLocalDir = Files.createTempDirectory("test",
                                              PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"))).toFile();
    url = "http://localhost:19121/api/v1";
    String branch = "main";
    String authType = "NONE";
    String defaultFs = alleyLocalDir.toURI().toString();
    String fsImpl = org.apache.hadoop.fs.LocalFileSystem.class.getName();

    hadoopConfig = new Configuration();
    hadoopConfig.set("nessie.url", url);
    hadoopConfig.set("nessie.view-branch", branch);
    hadoopConfig.set("nessie.auth.type", authType);
    hadoopConfig.set("fs.defaultFS", defaultFs);
    hadoopConfig.set("fs.file.impl", fsImpl);

    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
                        .config("spark.hadoop.nessie.url", url)
                        .config("spark.hadoop.nessie.view-branch", branch)
                        .config("spark.hadoop.nessie.auth.type", authType)
                        .config("spark.hadoop.fs.defaultFS", defaultFs)
                        .config("spark.hadoop.fs.file.impl", fsImpl)
                        .config("spark.sql.catalog.nessie", NessieSparkCatalog.class.getName())
                        .getOrCreate();
    catalog = new NessieCatalog(hadoopConfig);
  }

  @AfterAll
  static void tearDown() throws Exception {
    NessieClient client = new NessieClient(AuthType.NONE, url, null, null);
    client.deleteBranch(client.getBranch("main"));
    client.close();
    catalog.close();
    catalog = null;
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  void testAPI() throws IOException {
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema).location());
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] {"bar1.1", "bar2.1"});
    stringAsList.add(new String[] {"bar1.2", "bar2.2"});

    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map(RowFactory::create);

    // Create schema
    StructType schema = DataTypes
        .createStructType(new StructField[] {
          DataTypes.createStructField("foe1", DataTypes.StringType, false),
          DataTypes.createStructField("foe2", DataTypes.StringType, false)
        });

    Dataset<Row> dataDF = spark.sqlContext().createDataFrame(rowRDD, schema);

    dataDF.write().format("nessie-iceberg").mode("append").save(TABLE_IDENTIFIER.toString());

    Dataset<Row> table1 = spark.read().format("nessie-iceberg").load(TABLE_IDENTIFIER.toString());
    assertEquals("can read and write", transform(dataDF), transform(table1.sort("foe1")));
    tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
    catalog.refreshBranch();
    catalog.dropTable(TABLE_IDENTIFIER, false);
  }

  @Test
  void testSQL() {
    String tableId = "nessie." + TABLE_IDENTIFIER.toString();
    spark.sql(String.format("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableId));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableId));
    List<Object[]> expected = ImmutableList.of(new Object[]{1L, "a"}, new Object[]{2L, "b"}, new Object[]{3L, "c"});
    Dataset<Row> table2 = spark.sql("select * from " + tableId);
    assertEquals("can read and write sql", expected, transform(table2.sort("id")));
    spark.sql("DROP TABLE IF EXISTS " + tableId);
  }

  protected List<Object[]> transform(Dataset<Row> table) {
    return table.collectAsList().stream()
                .map(row -> IntStream.range(0, row.size())
                                     .mapToObj(pos -> row.isNullAt(pos) ? null : row.get(pos))
                                     .toArray(Object[]::new)
                ).collect(Collectors.toList());
  }

  void assertEquals(String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
    Assertions.assertEquals(expectedRows.size(), actualRows.size(), context + ": number of results should match");
    for (int row = 0; row < expectedRows.size(); row += 1) {
      Object[] expected = expectedRows.get(row);
      Object[] actual = actualRows.get(row);
      Assertions.assertEquals(expected.length, actual.length, "Number of columns should match");
      for (int col = 0; col < actualRows.get(row).length; col += 1) {
        if (expected[col] != ANY) {
          Assertions.assertEquals(expected[col], actual[col], context + ": row " + row + " col " + col + " contents should match");
        }
      }
    }
  }

}

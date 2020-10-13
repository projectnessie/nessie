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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.tests.AbstractSparkTest;
import com.dremio.nessie.iceberg.NessieCatalog;
import com.dremio.nessie.model.Branch;
import com.google.common.collect.ImmutableList;

class ITIcebergSpark3 extends AbstractSparkTest {

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema schema = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
                                                                      required(2, "foe2", Types.StringType.get())).fields());

  @TempDir
  static File icebergLocalDir;


  protected Path tableLocation;
  protected NessieCatalog catalog;
  private NessieClient client;

  @BeforeAll
  static void source() {
    String defaultFs = icebergLocalDir.toURI().toString();
    String fsImpl = org.apache.hadoop.fs.LocalFileSystem.class.getName();

    hadoopConfig.set("fs.defaultFS", defaultFs);
    hadoopConfig.set("fs.file.impl", fsImpl);
    conf.set("spark.sql.catalog.nessie", NessieIcebergSparkCatalog.class.getName());
    conf.set("spark.hadoop.fs.defaultFS", defaultFs);
    conf.set("spark.hadoop.fs.file.impl", fsImpl);
  }

  /**
   * make sure the fs is set correctly for writing Iceberg tables.
   */
  @BeforeEach
  public void icebergFs() {
    client = new NessieClient(AuthType.NONE, url, null, null);
    catalog = new NessieCatalog(hadoopConfig);
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema).location());
  }

  @AfterEach
  public void tearDown() throws Exception {
    tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
    catalog.refresh();
    catalog.dropTable(TABLE_IDENTIFIER, false);
    catalog.close();
    catalog = null;
    client.close();
    client = null;
  }

  @Test
  void testBranchHash() throws IOException {
    client.getTreeApi().assignBranch("test", null, client.getTreeApi().getReferenceByName("main").getHash());
    Branch branch = (Branch) client.getTreeApi().getReferenceByName("test");
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

    dataDF.write().format("iceberg").mode("append").save(TABLE_IDENTIFIER.toString() + "@test");

    Dataset<Row> table1 = spark.read().format("iceberg").load(TABLE_IDENTIFIER.toString() + "@test");
    assertEquals("can read and write", transform(dataDF), transform(table1.sort("foe1")));

    Dataset<Row> table2 = spark.read().format("iceberg").option("nessie.ref", "test").load(TABLE_IDENTIFIER.toString());
    assertEquals("can read and write", transform(dataDF), transform(table2.sort("foe1")));


    Branch branchFirstCommit = (Branch) client.getTreeApi().getReferenceByName("test");
    dataDF.write().format("iceberg").mode("append").save(TABLE_IDENTIFIER.toString() + "@test");
    Dataset<Row> table3 = spark.read()
                               .format("iceberg")
                               .option("nessie.ref", branchFirstCommit.getHash())
                               .load(TABLE_IDENTIFIER.toString());

    assertEquals("can read and write", transform(dataDF), transform(table3.sort("foe1")));


    Dataset<Row> table4 = spark.read()
                               .format("iceberg")
                               .load(String.format("%s@%s", TABLE_IDENTIFIER.toString(), branchFirstCommit.getHash()));
    assertEquals("can read and write", transform(table4.sort("foe1")), transform(table3.sort("foe1")));

    client.getTreeApi().deleteBranch(branch.getName(), client.getTreeApi().getReferenceByName("test").getHash());
  }

  @Test
  void testAPI() throws IOException {
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

    dataDF.write().format("iceberg").mode("append").save(TABLE_IDENTIFIER.toString());

    Dataset<Row> table1 = spark.read().format("iceberg").load(TABLE_IDENTIFIER.toString());
    assertEquals("can read and write", transform(dataDF), transform(table1.sort("foe1")));
  }

  @Test
  void testSQL() {
    String tableId = "nessie.x" + TABLE_IDENTIFIER.toString();
    spark.sql(String.format("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableId));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableId));
    List<Object[]> expected = ImmutableList.of(new Object[]{1L, "a"}, new Object[]{2L, "b"}, new Object[]{3L, "c"});
    Dataset<Row> table2 = spark.sql("select * from " + tableId);
    assertEquals("can read and write sql", expected, transform(table2.sort("id")));
    Dataset<Row> table3 = spark.sql("select * from nessie.xdb.`tbl@main`");
    assertEquals("can read and write sql", expected, transform(table3.sort("id")));
    spark.sql("DROP TABLE IF EXISTS " + tableId);
  }
}

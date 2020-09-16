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
package com.dremio.nessie.spark;

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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.client.tests.AbstractSparkTest;
import com.dremio.nessie.deltalake.NessieLogFileMetaParser;
import com.dremio.nessie.deltalake.NessieLogStore;
import com.dremio.nessie.iceberg.NessieCatalog;

import io.delta.tables.DeltaTable;

class ITNessieSpark3 extends AbstractSparkTest {

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema schema = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
                                                                      required(2, "foe2", Types.StringType.get())).fields());

  @TempDir
  static File icebergLocalDir;
  @TempDir
  static File deltaLocalDir;

  protected Path tableLocation;
  protected NessieCatalog catalog;



  @BeforeAll
  protected static void createDelta() {
    String defaultFs = icebergLocalDir.toURI().toString() + "/iceberg";
    String fsImpl = org.apache.hadoop.fs.LocalFileSystem.class.getName();
    hadoopConfig.set("fs.defaultFS", defaultFs);
    hadoopConfig.set("fs.file.impl", fsImpl);
    conf.set("spark.delta.logStore.class", NessieLogStore.class.getCanonicalName())
        .set("spark.delta.logFileHandler.class", NessieLogFileMetaParser.class.getCanonicalName())
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", NessieLogFileMetaParser.class.getCanonicalName())
        .set("spark.sql.catalog.nessie", NessieSparkCatalog.class.getName())
        .set("spark.hadoop.fs.defaultFS", defaultFs)
        .set("spark.hadoop.fs.file.impl", fsImpl);
  }

  /**
   * make sure the fs is set correctly for writing Iceberg tables.
   */
  @BeforeEach
  public void icebergFs() {
    catalog = new NessieCatalog(hadoopConfig);
  }

  @AfterEach
  public void tearDown() throws Exception {
    catalog.close();
    catalog = null;
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

    dataDF.write().format("nessie").option("nessie.file.type", "iceberg").mode("append").save(TABLE_IDENTIFIER.toString());
    dataDF.write().format("nessie").option("nessie.file.type", "delta").mode("overwrite").save(deltaLocalDir.getAbsolutePath());


    Dataset<Row> table1 = spark.read().format("nessie").load(TABLE_IDENTIFIER.toString());
    Dataset<Row> table2 = spark.read().format("nessie").load(deltaLocalDir.getAbsolutePath());
    assertEquals("can read and write", transform(table2.sort("foe1")), transform(table1.sort("foe1")));

    DeltaTable target = DeltaTable.forPath(spark, deltaLocalDir.getAbsolutePath());
    target.delete();

    tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
    catalog.refresh();
    catalog.dropTable(TABLE_IDENTIFIER, false);
  }

  @Disabled("Nessie Catalog is not yet correct. Skipping SQL for now")
  @Test
  void testSQL() {
    String tableId = "nessie." + TABLE_IDENTIFIER.toString();
    spark.sql(String.format("CREATE TABLE %s (id BIGINT NOT NULL, data STRING) USING iceberg", tableId));
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableId));
    String tableId2 = "delta_table";
    String path = deltaLocalDir.getAbsolutePath() + "/delta";
    spark.sql(String.format("CREATE TABLE nessie.%s (id BIGINT NOT NULL, data STRING) USING delta LOCATION '%s'", tableId2, path));
    spark.sql(String.format("INSERT INTO nessie.`%s` VALUES (1, 'a'), (2, 'b'), (3, 'c')", path));

    Dataset<Row> table1 = spark.sql("select * from " + tableId);
    Dataset<Row> table2 = spark.sql("select * from `" + path + "`");
    assertEquals("can read and write sql", transform(table1.sort("id")), transform(table2.sort("id")));

    spark.sql("DROP TABLE IF EXISTS " + tableId);
    spark.sql("DROP TABLE IF EXISTS `" + path + "`");
  }
}

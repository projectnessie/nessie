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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.client.tests.AbstractSparkTest;
import com.dremio.nessie.iceberg.NessieCatalog;

public class ITIcebergSpark extends AbstractSparkTest {

  private static final Object ANY = new Object();
  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final Schema schema = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
                                                                      required(2, "foe2", Types.StringType.get())).fields());

  @TempDir
  File alleyLocalDir;


  protected Path tableLocation;
  protected NessieCatalog catalog;

  /**
   * make sure the fs is set correctly for writing Iceberg tables.
   */
  @BeforeEach
  public void icebergFs() {
    String defaultFs = alleyLocalDir.toURI().toString();
    String fsImpl = org.apache.hadoop.fs.LocalFileSystem.class.getName();
    spark.sparkContext().conf().set("spark.hadoop.fs.defaultFS", defaultFs);
    spark.sparkContext().conf().set("spark.hadoop.fs.file.impl", fsImpl);
    hadoopConfig.set("fs.defaultFS", defaultFs);
    hadoopConfig.set("fs.file.impl", fsImpl);
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

    JavaRDD<Row> rowRDD = sparkContext
        .parallelize(stringAsList)
        .map(RowFactory::create);

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

    tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
    catalog.refresh();
    catalog.dropTable(TABLE_IDENTIFIER, false);
  }

  protected List<Object[]> transform(Dataset<Row> table) {

    return table.collectAsList().stream()
                .map(row -> IntStream.range(0, row.size())
                                     .mapToObj(pos -> row.isNullAt(pos) ? null : row.get(pos))
                                     .toArray(Object[]::new)
                ).collect(Collectors.toList());
  }

  protected void assertEquals(String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
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

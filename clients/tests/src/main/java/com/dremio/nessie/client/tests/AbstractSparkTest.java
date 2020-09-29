package com.dremio.nessie.client.tests;/*
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


import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractSparkTest {
  private static final Object ANY = new Object();
  public static final String CONF_NESSIE_URL = "nessie.url";
  public static final String CONF_NESSIE_USERNAME = "nessie.username";
  public static final String CONF_NESSIE_PASSWORD = "nessie.password";
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.auth_type";
  public static final String NESSIE_AUTH_TYPE_DEFAULT = "BASIC";
  public static final String CONF_NESSIE_REF = "nessie.ref";
  protected static SparkConf conf = new SparkConf();

  protected static SparkSession spark;
  protected static Configuration hadoopConfig = new Configuration();
  protected static String url = "http://localhost:19121/api/v1";

  @BeforeEach
  protected void create() throws IOException {
    String branch = "main";
    String authType = "NONE";

    hadoopConfig.set(CONF_NESSIE_URL, url);
    hadoopConfig.set(CONF_NESSIE_REF, branch);
    hadoopConfig.set(CONF_NESSIE_AUTH_TYPE, authType);

    conf.set("spark.hadoop." + CONF_NESSIE_URL, url)
        .set("spark.hadoop." + CONF_NESSIE_REF, branch)
        .set("spark.hadoop." + CONF_NESSIE_AUTH_TYPE, authType)
        .set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic");
    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(conf)
                        .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }


  protected static List<Object[]> transform(Dataset<Row> table) {

    return table.collectAsList().stream()
                .map(row -> IntStream.range(0, row.size())
                                     .mapToObj(pos -> row.isNullAt(pos) ? null : row.get(pos))
                                     .toArray(Object[]::new)
                ).collect(Collectors.toList());
  }

  protected static void assertEquals(String context, List<Object[]> expectedRows, List<Object[]> actualRows) {
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

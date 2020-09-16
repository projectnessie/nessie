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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class AbstractSparkTest {
  public static final String CONF_NESSIE_URL = "nessie.url";
  public static final String CONF_NESSIE_USERNAME = "nessie.username";
  public static final String CONF_NESSIE_PASSWORD = "nessie.password";
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.auth_type";
  public static final String NESSIE_AUTH_TYPE_DEFAULT = "BASIC";
  public static final String CONF_NESSIE_REF = "nessie.ref";

  protected static SparkSession spark;
  protected static Configuration hadoopConfig;
  protected static String url;

  @BeforeAll
  static void create() throws IOException {
    url = "http://localhost:19121/api/v1";
    String branch = "main";
    String authType = "NONE";

    hadoopConfig = new Configuration();
    hadoopConfig.set(CONF_NESSIE_URL, url);
    hadoopConfig.set(CONF_NESSIE_REF, branch);
    hadoopConfig.set(CONF_NESSIE_AUTH_TYPE, authType);

    spark = SparkSession.builder()
                        .master("local[2]")
                        .config(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic")
                        .config("spark.hadoop." + CONF_NESSIE_URL, url)
                        .config("spark.hadoop." + CONF_NESSIE_REF, branch)
                        .config("spark.hadoop." + CONF_NESSIE_AUTH_TYPE, authType)
                        .getOrCreate();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

}

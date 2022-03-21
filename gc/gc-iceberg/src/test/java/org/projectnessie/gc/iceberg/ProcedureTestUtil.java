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
package org.projectnessie.gc.iceberg;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

final class ProcedureTestUtil {

  private ProcedureTestUtil() {}

  static SparkSession getSessionWithGcCatalog(String uri, String location, String catalogClass) {
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.catalog.nessie.uri", uri)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.warehouse", location)
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        // Use the catalogClass which is loaded with the GC stored procedures in
        // "nessie_gc" namespace.
        .set("spark.sql.catalog.nessie", catalogClass)
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions");
    SparkSession spark =
        SparkSession.builder()
            .appName("test-nessie-gc")
            .master("local[2]")
            .config(conf)
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    return spark;
  }
}

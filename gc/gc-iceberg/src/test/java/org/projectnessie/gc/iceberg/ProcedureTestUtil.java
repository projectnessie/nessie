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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.gc.iceberg.GCProcedureUtil.NAMESPACE;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

final class ProcedureTestUtil {

  private ProcedureTestUtil() {}

  static SparkSession getSessionWithGcCatalog(
      String uri, String location, String catalogClass, String defaultBranch) {
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.catalog.nessie.uri", uri)
        .set("spark.sql.catalog.nessie.ref", defaultBranch)
        .set("spark.sql.catalog.nessie.warehouse", location)
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        // Use the catalogClass which is loaded with the GC stored procedures in
        // "nessie_gc" namespace.
        .set("spark.sql.catalog.nessie", catalogClass)
        .set(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSpark32SessionExtensions");
    SparkSession spark =
        SparkSession.builder()
            .appName("test-nessie-gc")
            .master("local[2]")
            .config(conf)
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    return spark;
  }

  static String performGcWithProcedure(
      SparkSession sparkSession,
      String catalogName,
      String gcBranchName,
      String outputTableIdentifier,
      String uri,
      Instant cutoffTimeStamp,
      Instant deadReferenceCutoffTime,
      Map<String, Instant> cutOffTimeStampPerRef) {
    // Example Query:
    // CALL nessie.nessie_gc.identify_expired_contents(
    //  default_cut_off_timestamp => TIMESTAMP '2022-05-02 16:39:57.258687',
    //  dead_reference_cut_off_timestamp => TIMESTAMP '2022-05-02 20:40:57.258687',
    //  nessie_catalog_name => 'nessie',
    //  output_branch_name => 'gcRef',
    //  output_table_identifier => 'singleRefRenameTableBeforeCutoff.gc_results',
    //  nessie_client_configurations => map('nessie.uri','http://localhost:51429/'),
    //  bloom_filter_expected_entries => 5)
    if (deadReferenceCutoffTime == null) {
      deadReferenceCutoffTime = cutoffTimeStamp;
    }
    StringBuilder sb = new StringBuilder();
    String commonParams =
        String.format(
            "CALL %s.%s.%s("
                + "default_cut_off_timestamp => TIMESTAMP '%s', "
                + "dead_reference_cut_off_timestamp => TIMESTAMP '%s', "
                + "nessie_catalog_name => '%s', "
                + "output_branch_name => '%s', "
                + "output_table_identifier => '%s', "
                + "nessie_client_configurations => map('%s','%s'), "
                + "bloom_filter_expected_entries => %d",
            catalogName,
            NAMESPACE,
            IdentifyExpiredContentsProcedure.PROCEDURE_NAME,
            //
            Timestamp.from(cutoffTimeStamp),
            Timestamp.from(deadReferenceCutoffTime),
            catalogName,
            gcBranchName,
            outputTableIdentifier,
            CONF_NESSIE_URI,
            uri,
            500);
    sb.append(commonParams);
    if (cutOffTimeStampPerRef != null && !cutOffTimeStampPerRef.isEmpty()) {
      List<String> entries = new ArrayList<>();
      cutOffTimeStampPerRef.forEach(
          (key, value) -> {
            entries.add("'" + key + "'");
            entries.add(String.format("TIMESTAMP '%s'", Timestamp.from(value)));
          });
      String cutoffTimePerReference = String.join(",", entries);
      String perRefCutoff =
          String.format(",reference_cut_off_timestamps => map(%s)", cutoffTimePerReference);
      sb.append(perRefCutoff);
    }
    sb.append(")");
    // execute the call procedure
    return sparkSession.sql(sb.toString()).collectAsList().get(0).getString(0);
  }

  static void createBranch(
      SparkSession sparkSession, String catalogName, String newBranch, String fromBranchHead) {
    ProcedureTestUtil.sql(
        sparkSession,
        "CREATE BRANCH IF NOT EXISTS %s IN %s FROM %s",
        newBranch,
        catalogName,
        fromBranchHead);
  }

  static void createTag(
      SparkSession sparkSession, String catalogName, String tagName, String fromBranchHead) {
    ProcedureTestUtil.sql(
        sparkSession,
        "CREATE TAG IF NOT EXISTS %s IN %s FROM %s",
        tagName,
        catalogName,
        fromBranchHead);
  }

  static void dropBranch(SparkSession sparkSession, String catalogName, String branchHead) {
    ProcedureTestUtil.sql(sparkSession, "DROP BRANCH %s IN %s", branchHead, catalogName);
  }

  static void dropTag(SparkSession sparkSession, String catalogName, String tagName) {
    ProcedureTestUtil.sql(sparkSession, "DROP TAG %s IN %s", tagName, catalogName);
  }

  static void useReference(SparkSession sparkSession, String catalogName, String branchHead) {
    ProcedureTestUtil.sql(sparkSession, "USE REFERENCE %s IN %s", branchHead, catalogName);
  }

  static void createTable(
      SparkSession sparkSession,
      String catalogName,
      String namespace,
      String tableName,
      String formatVersion) {
    if (formatVersion.equals("v2")) {
      ProcedureTestUtil.sql(
          sparkSession,
          "CREATE TABLE %s(id int) USING ICEBERG TBLPROPERTIES('format-version'='2', 'write.delete"
              + ".mode'='merge-on-read')",
          catalogName + "." + namespace + "." + tableName);
    } else {
      createTable(sparkSession, catalogName, namespace, tableName);
    }
  }

  static void createTable(
      SparkSession sparkSession, String catalogName, String namespace, String tableName) {
    ProcedureTestUtil.sql(
        sparkSession,
        "CREATE TABLE %s(id int) USING ICEBERG",
        catalogName + "." + namespace + "." + tableName);
  }

  static void dropTable(
      SparkSession sparkSession, String catalogName, String namespace, String tableName) {
    ProcedureTestUtil.sql(
        sparkSession, "DROP TABLE %s", catalogName + "." + namespace + "." + tableName);
  }

  static void commit(
      SparkSession sparkSession,
      String catalogName,
      String namespace,
      String tableName,
      String value) {
    ProcedureTestUtil.sql(
        sparkSession,
        "INSERT INTO %s SELECT %s",
        catalogName + "." + namespace + "." + tableName,
        value);
  }

  static void commit(
      SparkSession sparkSession, String catalogName, String namespace, String tableName) {
    commit(sparkSession, catalogName, namespace, tableName, "42");
  }

  @FormatMethod
  static Dataset<Row> sql(
      SparkSession sparkSession, @FormatString String sqlStatement, Object... args) {
    String sql = String.format(sqlStatement, args);
    return sparkSession.sql(sql);
  }
}

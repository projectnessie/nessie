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
import static org.projectnessie.gc.iceberg.GcProcedureUtil.NAMESPACE;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  static String performGcWithProcedure(
      SparkSession sparkSession,
      String catalogName,
      String gcBranchName,
      String tableIdentifier,
      String uri,
      Instant cutoffTimeStamp,
      boolean disableCommitProtection,
      Instant deadReferenceCutoffTime,
      Map<String, Instant> cutOffTimeStampPerRef) {
    int commitProtectionTimeInHours = disableCommitProtection ? 0 : 2;
    // Example Query:
    // CALL nessie.nessie_gc.identify_expired_snapshots(
    //  default_cut_off_timestamp => 1647391705,
    //  nessie_catalog_name => 'nessie',
    //  output_branch_name => 'gcRef',
    //  output_table_identifier => 'singleRefRenameTableBeforeCutoff.gc_results',
    //  nessie_client_configurations => map('nessie.uri','http://localhost:51429/'),
    //  commit_protection_time_in_hours => 0,
    //  bloom_filter_expected_entries => 5)
    StringBuilder sb = new StringBuilder();
    String commonParams =
        String.format(
            "CALL %s.%s.%s("
                + "default_cut_off_timestamp => TIMESTAMP '%s', "
                + "nessie_catalog_name => '%s', "
                + "output_branch_name => '%s', "
                + "output_table_identifier => '%s', "
                + "nessie_client_configurations => map('%s','%s'), "
                + "commit_protection_time_in_hours => %d, "
                + "bloom_filter_expected_entries => %d",
            catalogName,
            NAMESPACE,
            IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME,
            //
            Timestamp.from(cutoffTimeStamp),
            catalogName,
            gcBranchName,
            tableIdentifier,
            CONF_NESSIE_URI,
            uri,
            commitProtectionTimeInHours,
            5);
    sb.append(commonParams);
    if (deadReferenceCutoffTime != null) {
      String deadReferenceCutoff =
          String.format(
              ",dead_reference_cut_off_timestamp => TIMESTAMP '%s'",
              Timestamp.from(deadReferenceCutoffTime));
      sb.append(deadReferenceCutoff);
    }
    if (cutOffTimeStampPerRef != null) {
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
}

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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;
import static org.projectnessie.gc.iceberg.GcProcedureUtil.NAMESPACE;

import java.io.File;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.gc.base.AbstractRestGCTest;
import org.projectnessie.gc.base.IdentifiedResultsRepo;

/** Tests all the cases from {@link AbstractRestGCTest} using stored procedure. */
public abstract class AbstractIdentifyProcedure extends AbstractRestGCTest {

  @TempDir File tempDir;

  static final String CATALOG_NAME = "nessie";
  static final String GC_BRANCH_NAME = "gcBranch";
  static final String GC_TABLE_NAME = "gc_results";
  static final String GC_SPARK_CATALOG = "org.projectnessie.gc.iceberg.NessieIcebergGcSparkCatalog";

  @Override
  protected SparkSession getSparkSession() {
    return ProcedureTestUtil.getSessionWithGcCatalog(
        getUri().toString(), tempDir.toURI().toString(), GC_SPARK_CATALOG);
  }

  @Override
  protected void performGc(
      String prefix,
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      List<Row> expectedDataSet,
      boolean disableCommitProtection,
      Instant deadReferenceCutoffTime) {
    try (SparkSession session = getSparkSession()) {
      String runId =
          ProcedureTestUtil.performGcWithProcedure(
              session,
              CATALOG_NAME,
              GC_BRANCH_NAME,
              prefix + "." + GC_TABLE_NAME,
              getUri().toString(),
              cutoffTimeStamp,
              disableCommitProtection,
              deadReferenceCutoffTime,
              cutOffTimeStampPerRef);
      IdentifiedResultsRepo actualIdentifiedResultsRepo =
          new IdentifiedResultsRepo(
              session, CATALOG_NAME, GC_BRANCH_NAME, prefix + "." + GC_TABLE_NAME);
      Dataset<Row> actualRowDataset =
          actualIdentifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
      verify(actualRowDataset, expectedDataSet, session, actualIdentifiedResultsRepo.getSchema());
    }
  }

  // As this class extends AbstractRestGCTest, positive scenarios are covered from that.
  // Below is the testcase to verify the procedure.
  @Test
  public void testInvalidScenarios() {
    try (SparkSession sparkSession = getSparkSession()) {
      // test using a namespace that doesn't contain the GC stored procedures
      assertThatThrownBy(
              () ->
                  sparkSession
                      .sql(
                          String.format(
                              "CALL %s.%s.%s("
                                  + "default_cut_off_timestamp => %d, "
                                  + "nessie_catalog_name => '%s', "
                                  + "output_branch_name => '%s', "
                                  + "output_table_identifier => '%s', "
                                  + "nessie_client_configurations => map('%s','%s'), "
                                  + "bloom_filter_expected_entries => %d)",
                              CATALOG_NAME,
                              // Use namespace that doesn't contain the GC stored procedures
                              "other_namespace",
                              IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME,
                              Instant.now().getEpochSecond(),
                              CATALOG_NAME,
                              GC_BRANCH_NAME,
                              GC_TABLE_NAME,
                              CONF_NESSIE_URI,
                              getUri().toString(),
                              5))
                      .collectAsList())
          .isInstanceOf(NoSuchProcedureException.class)
          .hasMessageContaining("Procedure other_namespace.identify_expired_snapshots not found");

      // skip passing the required argument 'default_cut_off_timestamp'
      assertThatThrownBy(
              () ->
                  sparkSession
                      .sql(
                          String.format(
                              "CALL %s.%s.%s("
                                  + "nessie_catalog_name => '%s', "
                                  + "output_branch_name => '%s', "
                                  + "output_table_identifier => '%s', "
                                  + "nessie_client_configurations => map('%s','%s'), "
                                  + "bloom_filter_expected_entries => %d)",
                              CATALOG_NAME,
                              NAMESPACE,
                              IdentifyExpiredSnapshotsProcedure.PROCEDURE_NAME,
                              CATALOG_NAME,
                              GC_BRANCH_NAME,
                              GC_TABLE_NAME,
                              CONF_NESSIE_URI,
                              getUri().toString(),
                              5))
                      .collectAsList())
          .isInstanceOf(AnalysisException.class)
          .hasMessageContaining("Missing required parameters: [default_cut_off_timestamp]");
    }
  }
}

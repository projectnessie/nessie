/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.base;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableTableReference;

public abstract class AbstractRestGCRepoTest extends AbstractRestGCTest {

  @Test
  public void testGCRepoMultipleRuns() {
    String catalogName = "nessie";
    String namespace = "db2";
    String tableName = "identified_results";
    String identifier = namespace + "." + tableName;
    String gcBranchName = "someGcBranch";
    String catalogAndIdentifierWithReference =
        getCatalogAndIdentifierWithReference(catalogName, namespace, tableName, gcBranchName);
    getOrCreateEmptyBranch(getApi(), gcBranchName);
    try (SparkSession sparkSession = getSparkSession()) {
      IdentifiedResultsRepo identifiedResultsRepo =
          new IdentifiedResultsRepo(sparkSession, catalogName, gcBranchName, identifier);
      try {
        List<String> runIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
          String runId = UUID.randomUUID().toString();
          Timestamp startAt = Timestamp.from(Instant.now());
          runIds.add(runId);
          List<Row> rows = createRows(runId, startAt, i + 1);
          Dataset<Row> dataset =
              sparkSession.createDataFrame(rows, identifiedResultsRepo.getSchema());
          identifiedResultsRepo.writeToOutputTable(dataset);
        }
        AtomicInteger expectedRowCount = new AtomicInteger(1);
        runIds.forEach(
            runId -> {
              Dataset<Row> identifiedResult =
                  identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
              assertThat(identifiedResult.collectAsList().size()).isEqualTo(expectedRowCount.get());
              expectedRowCount.getAndIncrement();
            });
        // validate the latest run id
        assertThat(identifiedResultsRepo.getLatestCompletedRunID().get())
            .isEqualTo(runIds.get(runIds.size() - 1));
      } finally {
        sparkSession.sql(String.format("DROP TABLE %s", catalogAndIdentifierWithReference));
      }
    }
  }

  private List<Row> createRows(String runId, Timestamp startAt, int rowCount) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      String contentId = "SomeContentId_" + i;
      String metadata = "file1";
      IcebergTable content = IcebergTable.of(metadata, 42, 42, 42, 42, contentId);
      String refName = "someRef";
      rows.add(
          RowFactory.create(
              startAt,
              runId,
              contentId,
              content.getType().name(),
              content.getSnapshotId(),
              refName,
              null,
              null,
              null));
    }
    return rows;
  }

  private static String getCatalogAndIdentifierWithReference(
      String catalogName, String namespace, String tableName, String gcBranchName) {
    return catalogName
        + "."
        + namespace
        + "."
        + ImmutableTableReference.builder().name(tableName).reference(gcBranchName).build();
  }
}

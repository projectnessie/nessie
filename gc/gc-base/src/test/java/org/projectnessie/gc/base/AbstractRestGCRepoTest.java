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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableTableReference;

public abstract class AbstractRestGCRepoTest extends AbstractRestGC {

  private final String catalogName = "nessie";
  private final String identifierNameSpace = "db1";
  private final String identifierTableName = "identified_results";
  private final String identifier = identifierNameSpace + "." + identifierTableName;
  private final String gcRefName = "someGcRef";
  private final String catalogAndIdentifierWithReference = getCatalogAndIdentifierWithReference();

  @Test
  public void testGCRepoInProgressAndSuccess() {
    checkAndCreateEmptyReference(getApi(), gcRefName);
    SparkSession sparkSession = null;
    try {
      sparkSession = getSparkSession();
      IdentifiedResultsRepo identifiedResultsRepo =
          new IdentifiedResultsRepo(sparkSession, catalogName, gcRefName, identifier);
      String runId = UUID.randomUUID().toString();
      Timestamp startAt = Timestamp.from(Instant.now());
      // write 5 rows to table without a marker row
      writeRows(sparkSession, identifiedResultsRepo, getRows(runId, startAt, 5));

      // try to collect in-progress results.
      Dataset<Row> identifiedResult = identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
      // should not collect any results as write is in-progress.
      assertThat(identifiedResult.collectAsList()).isEmpty();

      // latest completed run id should not be present.
      assertThat(identifiedResultsRepo.getLatestCompletedRunID()).isNull();

      // write marker row
      writeMarkerRow(sparkSession, identifiedResultsRepo, runId);

      assertThat(identifiedResultsRepo.getLatestCompletedRunID()).isEqualTo(runId);

      // collect results as marker row is written.
      identifiedResult = identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
      // should collect 5 rows.
      assertThat(identifiedResult.collectAsList().size()).isEqualTo(5);
    } finally {
      if (sparkSession != null) {
        sparkSession.sql(String.format("DROP TABLE %s", catalogAndIdentifierWithReference));
        sparkSession.close();
      }
    }
  }

  @Test
  public void testGCRepoMultipleRuns() {
    checkAndCreateEmptyReference(getApi(), gcRefName);
    SparkSession sparkSession = null;
    try {
      sparkSession = getSparkSession();
      IdentifiedResultsRepo identifiedResultsRepo =
          new IdentifiedResultsRepo(sparkSession, catalogName, gcRefName, identifier);
      List<String> runIds = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        String runId = UUID.randomUUID().toString();
        Timestamp startAt = Timestamp.from(Instant.now());
        runIds.add(runId);
        List<Row> rows = getRows(runId, startAt, i + 1);
        writeRows(sparkSession, identifiedResultsRepo, rows);
        writeMarkerRow(sparkSession, identifiedResultsRepo, runId);
      }
      AtomicInteger expectedRowCount = new AtomicInteger(1);
      runIds.forEach(
          runId -> {
            // collect results as marker row is written.
            Dataset<Row> identifiedResult =
                identifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
            // should collect expectedRowCount rows.
            assertThat(identifiedResult.collectAsList().size()).isEqualTo(expectedRowCount.get());
            expectedRowCount.getAndIncrement();
          });
      assertThat(identifiedResultsRepo.getLatestCompletedRunID())
          .isEqualTo(runIds.get(runIds.size() - 1));
    } finally {
      if (sparkSession != null) {
        sparkSession.sql(String.format("DROP TABLE %s", catalogAndIdentifierWithReference));
        sparkSession.close();
      }
    }
  }

  private List<Row> getRows(String runId, Timestamp startAt, int rowCount) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      String contentId = "SomeContentId_" + i;
      String metadata = "file1";
      IcebergTable content = IcebergTable.of(metadata, 42, 42, 42, 42, contentId);
      String refName = "someRef";
      rows.add(
          RowFactory.create(
              "GC_CONTENT",
              startAt,
              runId,
              contentId,
              content.getType().name(),
              content.getSnapshotId(),
              content.getMetadataLocation(),
              refName,
              null));
    }
    return rows;
  }

  private static List<Row> getMarkerRow(String runId) {
    return Collections.singletonList(
        RowFactory.create(
            "GC_MARK", Timestamp.from(Instant.now()), runId, null, null, null, null, null, null));
  }

  private void writeMarkerRow(
      SparkSession sparkSession, IdentifiedResultsRepo identifiedResultsRepo, String runId) {
    try {
      List<Row> row = getMarkerRow(runId);
      sparkSession
          .createDataFrame(row, identifiedResultsRepo.getSchema())
          .writeTo(catalogAndIdentifierWithReference)
          .append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeRows(
      SparkSession sparkSession, IdentifiedResultsRepo identifiedResultsRepo, List<Row> rows) {
    try {
      sparkSession
          .createDataFrame(rows, identifiedResultsRepo.getSchema())
          .writeTo(catalogAndIdentifierWithReference)
          .append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private String getCatalogAndIdentifierWithReference() {
    return catalogName
        + "."
        + identifierNameSpace
        + "."
        + ImmutableTableReference.builder().name(identifierTableName).reference(gcRefName).build();
  }
}

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
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.AbstractRest;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

public abstract class AbstractRestGC extends AbstractRest {

  @TempDir File tempDir;

  @NotNull
  List<LogEntry> fetchLogEntries(Branch branch, int numCommits) throws NessieNotFoundException {
    return getApi()
        .getCommitLog()
        .refName(branch.getName())
        .hashOnRef(branch.getHash())
        .fetch(FetchOption.ALL)
        .maxRecords(numCommits)
        .get()
        .getLogEntries();
  }

  void fillExpectedContents(Branch branch, int numCommits, List<Row> expected)
      throws NessieNotFoundException {
    fetchLogEntries(branch, numCommits).stream()
        .map(LogEntry::getOperations)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .filter(op -> op instanceof Put)
        .forEach(
            op -> {
              IcebergTable content = (IcebergTable) ((Put) op).getContent();
              // using only contentId, ref, snapshot id for validation
              // as metadata location will change based on new global state.
              expected.add(
                  RowFactory.create(
                      Timestamp.from(Instant.now()),
                      "dummyRunId",
                      content.getId(),
                      null,
                      content.getSnapshotId(),
                      branch.getName(),
                      null));
            });
  }

  protected void performGc(
      String prefix,
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      List<Row> expectedDataSet,
      Instant deadReferenceCutoffTime) {

    try (SparkSession sparkSession = getSparkSession()) {
      ImmutableGCParams.Builder builder = ImmutableGCParams.builder();
      final Map<String, String> options = new HashMap<>();
      options.put(CONF_NESSIE_URI, getUri().toString());
      ImmutableGCParams gcParams =
          builder
              .bloomFilterExpectedEntries(5L)
              .nessieClientConfigs(options)
              .deadReferenceCutOffTimeStamp(deadReferenceCutoffTime)
              .cutOffTimestampPerRef(cutOffTimeStampPerRef)
              .defaultCutOffTimestamp(cutoffTimeStamp)
              .nessieCatalogName("nessie")
              .outputBranchName("gcBranch")
              .outputTableIdentifier(prefix + ".gc_results")
              .build();
      GCImpl gc = new GCImpl(gcParams);
      String runId = gc.identifyExpiredContents(sparkSession);

      IdentifiedResultsRepo actualIdentifiedResultsRepo =
          new IdentifiedResultsRepo(
              sparkSession,
              gcParams.getNessieCatalogName(),
              gcParams.getOutputBranchName(),
              gcParams.getOutputTableIdentifier());
      Dataset<Row> actualRowDataset =
          actualIdentifiedResultsRepo.collectExpiredContentsAsDataSet(runId);
      // compare the expected contents against the actual gc output
      verify(
          actualRowDataset, expectedDataSet, sparkSession, actualIdentifiedResultsRepo.getSchema());
    }
  }

  protected SparkSession getSparkSession() {
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.catalog.nessie.uri", getUri().toString())
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.warehouse", tempDir.toURI().toString())
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
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

  protected void verify(
      Dataset<Row> actual, List<Row> expectedRows, SparkSession session, StructType schema) {
    Dataset<Row> expected = session.createDataFrame(expectedRows, schema);
    Dataset<Row> dfActual = actual.select("referenceName", "contentId", "snapshotId");
    Dataset<Row> dfExpected = expected.select("referenceName", "contentId", "snapshotId");
    // when both the dataframe is same, df.except() should return empty.
    assertThat(dfExpected.count()).isEqualTo(dfActual.count());
    assertThat(dfExpected.except(dfActual).collectAsList()).isEmpty();
  }

  CommitOutput commitSingleOp(
      String prefix,
      Reference branch,
      String currentHash,
      long snapshotId,
      String contentId,
      String contentKey,
      String metadataFile,
      IcebergTable previous,
      String beforeRename)
      throws NessieNotFoundException, NessieConflictException {
    IcebergTable meta =
        IcebergTable.of(
            prefix + "_" + metadataFile, snapshotId, 42, 42, 42, prefix + "_" + contentId);
    CommitMultipleOperationsBuilder multiOp =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(currentHash)
            .commitMeta(
                CommitMeta.builder()
                    .author("someone")
                    .message("some commit")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Put.of(ContentKey.of(prefix + "_" + contentKey), meta, previous));

    if (beforeRename != null) {
      multiOp.operation(Operation.Delete.of(ContentKey.of(prefix + "_" + beforeRename)));
    }

    String nextHash = multiOp.commit().getHash();
    assertThat(currentHash).isNotEqualTo(nextHash);
    return new CommitOutput(nextHash, meta);
  }

  CommitOutput dropTableCommit(
      String prefix, Reference branch, String currentHash, String contentKey)
      throws NessieNotFoundException, NessieConflictException {
    String nextHash =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(currentHash)
            .commitMeta(
                CommitMeta.builder()
                    .author("someone")
                    .message("some commit")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Delete.of(ContentKey.of(prefix + "_" + contentKey)))
            .commit()
            .getHash();
    assertThat(currentHash).isNotEqualTo(nextHash);
    return new CommitOutput(nextHash, null);
  }

  static final class CommitOutput {
    final String hash;
    final IcebergTable content;

    CommitOutput(String hash, IcebergTable content) {
      this.hash = hash;
      this.content = content;
    }
  }
}

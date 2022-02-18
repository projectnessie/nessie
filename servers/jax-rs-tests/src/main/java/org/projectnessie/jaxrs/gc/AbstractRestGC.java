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
package org.projectnessie.jaxrs.gc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.gc.base.ContentValues;
import org.projectnessie.gc.base.GCImpl;
import org.projectnessie.gc.base.IdentifiedResult;
import org.projectnessie.gc.base.ImmutableGCParams;
import org.projectnessie.jaxrs.AbstractRest;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

public abstract class AbstractRestGC extends AbstractRest {

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

  void fillExpectedContents(Branch branch, int numCommits, IdentifiedResult expected)
      throws NessieNotFoundException {
    fetchLogEntries(branch, numCommits).stream()
        .map(LogEntry::getOperations)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .filter(op -> op instanceof Put)
        .forEach(
            op -> {
              Content content = ((Put) op).getContent();
              expected.addContent(branch.getName(), content);
            });
  }

  void performGc(
      Instant cutoffTimeStamp,
      Map<String, Instant> cutOffTimeStampPerRef,
      IdentifiedResult expectedExpired,
      List<String> involvedRefs,
      boolean disableCommitProtection,
      Instant deadReferenceCutoffTime) {
    SparkSession spark =
        SparkSession.builder().appName("test-nessie-gc").master("local[2]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    try {
      ImmutableGCParams.Builder builder = ImmutableGCParams.builder();
      final Map<String, String> options = new HashMap<>();
      options.put(CONF_NESSIE_URI, getUri().toString());
      if (disableCommitProtection) {
        // disable commit protection for test purposes.
        builder.commitProtectionDuration(0);
      }
      ImmutableGCParams gcParams =
          builder
              .bloomFilterExpectedEntries(5L)
              .nessieClientConfigs(options)
              .deadReferenceCutOffTimeStamp(deadReferenceCutoffTime)
              .cutOffTimestampPerRef(cutOffTimeStampPerRef)
              .defaultCutOffTimestamp(cutoffTimeStamp)
              .build();
      GCImpl gc = new GCImpl(gcParams);
      IdentifiedResult identifiedResult = gc.identifyExpiredContents(spark);
      // compare the expected contents against the actual gc output
      verify(identifiedResult, expectedExpired, involvedRefs);
    } finally {
      spark.close();
    }
  }

  private void verify(
      IdentifiedResult identifiedResult,
      IdentifiedResult expectedExpired,
      List<String> involvedRefs) {
    assertThat(identifiedResult.getContentValues()).isNotNull();
    involvedRefs.forEach(
        ref ->
            compareContentsPerRef(
                identifiedResult.getContentValuesForReference(ref),
                expectedExpired.getContentValuesForReference(ref)));
  }

  void compareContentsPerRef(
      Map<String, ContentValues> actualMap, Map<String, ContentValues> expectedMap) {
    assertThat(actualMap.size()).isEqualTo(expectedMap.size());
    actualMap
        .keySet()
        .forEach(
            contentId -> {
              compareContents(actualMap.get(contentId), expectedMap.get(contentId));
            });
  }

  void compareContents(ContentValues actual, ContentValues expected) {
    if (actual == null && expected == null) {
      return;
    }
    assertThat(actual).isNotNull();
    assertThat(expected).isNotNull();
    assertThat(actual.getExpiredContents().size()).isEqualTo(expected.getExpiredContents().size());
    List<Content> expectedContents = new ArrayList<>(expected.getExpiredContents());
    expectedContents.sort(new ContentComparator());
    List<Content> actualContents = new ArrayList<>(actual.getExpiredContents());
    actualContents.sort(new ContentComparator());
    for (int i = 0; i < expectedContents.size(); i++) {
      compareContent(expectedContents.get(i), actualContents.get(i));
    }
  }

  void compareContent(Content actual, Content expected) {
    switch (expected.getType()) {
      case ICEBERG_TABLE:
        // exclude global state in comparison as it will change as per the commit and won't be same
        // as original.
        // compare only snapshot id for this content.
        assertThat((IcebergTable) actual)
            .extracting(IcebergTable::getSnapshotId)
            .isEqualTo(((IcebergTable) expected).getSnapshotId());
        break;
      case ICEBERG_VIEW:
        // exclude global state in comparison as it will change as per the commit and won't be same
        // as original.
        // compare only version id for this content.
        assertThat((IcebergView) actual)
            .extracting(IcebergView::getVersionId)
            .isEqualTo(((IcebergView) expected).getVersionId());
        break;
      default:
        // contents that doesn't have global state can be compared fully.
        assertThat(actual).isEqualTo(expected);
    }
  }

  static class ContentComparator implements Comparator<Content> {
    @Override
    public int compare(Content c1, Content c2) {
      switch (c1.getType()) {
        case ICEBERG_TABLE:
          return Long.compare(
              ((IcebergTable) c1).getSnapshotId(), ((IcebergTable) c2).getSnapshotId());
        case ICEBERG_VIEW:
          return Integer.compare(
              ((IcebergView) c1).getVersionId(), ((IcebergView) c2).getVersionId());
        default:
          return c1.getId().compareTo(c2.getId());
      }
    }
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

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
package org.projectnessie.versioned.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.HeadsAndForkPoints;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.ImmutableHeadsAndForkPoints;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.files.ZipArchiveExporter;
import org.projectnessie.versioned.transfer.files.ZipArchiveImporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

@ExtendWith(DatabaseAdapterExtension.class)
public abstract class AbstractITCommitLogOptimization {
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;

  static Stream<Arguments> multipleBranches() {
    return Stream.of(
        arguments(0, 0, 1, 0, 0, false),
        arguments(0, 0, 19, 0, 0, false),
        arguments(0, 0, 20, 0, 0, false),
        arguments(0, 0, 21, 0, 0, false),
        // 5
        arguments(1, 1, 1, 1, 1, false),
        arguments(1, 1, 1, 1, 0, true),
        arguments(1, 1, 0, 1, 1, false),
        arguments(1, 1, 0, 1, 0, true),
        arguments(1, 1, 0, 0, 0, false),
        // 10
        arguments(3, 5, 5, 5, 5, true),
        arguments(3, 5, 5, 1, 5, false),
        arguments(3, 5, 5, 0, 5, true),
        arguments(3, 5, 0, 5, 5, false),
        arguments(3, 5, 1, 5, 5, true),
        arguments(3, 5, 5, 5, 0, false),
        arguments(3, 5, 5, 5, 1, true),
        arguments(3, 5, 0, 0, 0, false),
        arguments(3, 5, 1, 1, 1, true),
        // 19
        arguments(3, 19, 19, 19, 19, true),
        arguments(3, 19, 19, 1, 19, false),
        arguments(3, 19, 19, 0, 19, true),
        arguments(3, 19, 0, 19, 19, false),
        arguments(3, 19, 1, 19, 19, true),
        arguments(3, 19, 19, 19, 0, false),
        arguments(3, 19, 19, 19, 1, true),
        arguments(3, 19, 0, 0, 0, false),
        arguments(3, 19, 1, 1, 1, true),
        // 28
        arguments(3, 20, 20, 20, 20, false),
        arguments(3, 20, 20, 1, 20, true),
        arguments(3, 20, 20, 0, 20, false),
        arguments(3, 20, 0, 20, 20, true),
        arguments(3, 20, 1, 20, 20, false),
        arguments(3, 20, 20, 20, 0, true),
        arguments(3, 20, 20, 20, 1, false),
        arguments(3, 20, 0, 0, 0, true),
        arguments(3, 20, 1, 1, 1, false),
        // 37
        arguments(3, 21, 21, 21, 21, true),
        arguments(3, 21, 21, 1, 21, false),
        arguments(3, 21, 21, 0, 21, true),
        arguments(3, 21, 0, 21, 21, false),
        arguments(3, 21, 1, 21, 21, true),
        arguments(3, 21, 21, 21, 0, false),
        arguments(3, 21, 21, 21, 1, true),
        arguments(3, 21, 0, 0, 0, false),
        arguments(3, 21, 1, 1, 1, true),
        // 46
        arguments(5, 19, 19, 19, 19, true),
        arguments(5, 20, 20, 20, 20, false),
        arguments(5, 21, 21, 21, 21, true));
  }

  @ParameterizedTest
  @MethodSource("multipleBranches")
  public void multipleBranches(
      int branches,
      int commitsOnBranches,
      int commitsAtBeginningOfTime,
      int commitsBetweenBranches,
      int commitsAtHead,
      boolean fullScan,
      @TempDir Path tempDir)
      throws Exception {
    long totalCommits = 0L;

    int commitSeqMain = 1;

    Hash parent = databaseAdapter.noAncestorHash();
    for (; commitSeqMain <= commitsAtBeginningOfTime; commitSeqMain++) {
      parent = addCommit(0, commitSeqMain).getCommit().getHash();
      totalCommits++;
    }

    for (int branch = 1; branch <= branches; branch++) {
      databaseAdapter.create(BranchName.of("branch-" + branch), parent);
      for (int i = 1; i <= commitsOnBranches; i++) {
        addCommit(branch, i);
        totalCommits++;
      }

      for (int i = 0; i < ((branch == branches) ? commitsAtHead : commitsBetweenBranches); i++) {
        parent = addCommit(0, commitSeqMain++).getCommit().getHash();
        totalCommits++;
      }
    }

    List<ReferenceInfo<ByteString>> namedRefs = allRefs();
    List<CommitLogEntry> scannedCommits = allCommits();

    Path file = tempDir.resolve("export.zip");
    ExportMeta exportMeta;
    try (ZipArchiveExporter zipExporter = ZipArchiveExporter.builder().outputFile(file).build()) {
      exportMeta =
          NessieExporter.builder()
              .exportFileSupplier(zipExporter)
              .databaseAdapter(databaseAdapter)
              .fullScan(fullScan)
              .build()
              .exportNessieRepository();
    }
    assertThat(exportMeta)
        .extracting(ExportMeta::getCommitCount, ExportMeta::getNamedReferencesCount)
        .containsExactly(totalCommits, 1L + branches);

    databaseAdapter.eraseRepo();
    databaseAdapter.initializeRepo("not-main");
    databaseAdapter.delete(BranchName.of("not-main"), Optional.empty());

    assertThat(allRefs()).isEmpty();
    assertThat(allCommits()).isEmpty();

    ImportResult importResult;
    try (ImportFileSupplier importFileSupplier =
        ZipArchiveImporter.builder().sourceZipFile(file).build()) {
      importResult =
          NessieImporter.builder()
              .importFileSupplier(importFileSupplier)
              .databaseAdapter(databaseAdapter)
              .build()
              .importNessieRepository();
    }

    assertThat(importResult)
        .extracting(
            ImportResult::exportMeta,
            ImportResult::importedCommitCount,
            ImportResult::importedReferenceCount)
        .containsExactly(exportMeta, totalCommits, 1L + branches);

    assertThat(allRefs()).containsExactlyInAnyOrderElementsOf(namedRefs);
    assertThat(scannedCommits)
        .map(CommitLogEntry::getHash)
        .containsExactlyInAnyOrderElementsOf(
            scannedCommits.stream().map(CommitLogEntry::getHash).collect(Collectors.toSet()));

    CommitLogOptimization.builder()
        .databaseAdapter(databaseAdapter)
        .headsAndForks(toHeadsAndForkPoints(importResult.headsAndForks()))
        .build()
        .optimize();

    assertThat(allRefs()).containsExactlyInAnyOrderElementsOf(namedRefs);
    assertThat(allCommits()).containsExactlyInAnyOrderElementsOf(scannedCommits);
  }

  static HeadsAndForkPoints toHeadsAndForkPoints(HeadsAndForks headsAndForks) {
    ImmutableHeadsAndForkPoints.Builder hfBuilder =
        ImmutableHeadsAndForkPoints.builder()
            .scanStartedAtInMicros(headsAndForks.getScanStartedAtInMicros());
    headsAndForks.getHeadsList().forEach(h -> hfBuilder.addHeads(Hash.of(h)));
    headsAndForks.getForkPointsList().forEach(h -> hfBuilder.addForkPoints(Hash.of(h)));
    return hfBuilder.build();
  }

  static List<ReferenceInfo<ByteString>> allRefs() throws Exception {
    try (Stream<ReferenceInfo<ByteString>> refs =
        databaseAdapter.namedRefs(GetNamedRefsParams.DEFAULT)) {
      return refs.collect(Collectors.toList());
    }
  }

  static List<CommitLogEntry> allCommits() {
    try (Stream<CommitLogEntry> commitScan = databaseAdapter.scanAllCommitLogEntries()) {
      return commitScan.collect(Collectors.toList());
    }
  }

  CommitResult<CommitLogEntry> addCommit(int branch, int commitSeq) throws Exception {
    return databaseAdapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(branch == 0 ? BranchName.of("main") : BranchName.of("branch-" + branch))
            .addPuts(
                KeyWithBytes.of(
                    ContentKey.of("branch-" + branch + "-commit-" + commitSeq),
                    ContentId.of("cid-" + branch + "-" + commitSeq),
                    (byte) DefaultStoreWorker.payloadForContent(Content.Type.ICEBERG_TABLE),
                    DefaultStoreWorker.instance()
                        .toStoreOnReferenceState(
                            IcebergTable.of(
                                "meta", 42L, 43, 44, 45, "cid-" + branch + "-" + commitSeq))))
            .commitMetaSerialized(
                CommitMetaSerializer.METADATA_SERIALIZER.toBytes(
                    CommitMeta.fromMessage("branch-" + branch + "-" + commitSeq)))
            .build());
  }
}

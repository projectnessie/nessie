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

import static java.lang.Integer.parseInt;
import static java.util.Collections.emptyList;
import static java.util.stream.Stream.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.versioned.transfer.ExportImportConstants.EXPORT_METADATA;
import static org.projectnessie.versioned.transfer.ExportImportConstants.HEADS_AND_FORKS;
import static org.projectnessie.versioned.transfer.ExportImportTestUtil.commitMeta;
import static org.projectnessie.versioned.transfer.ExportImportTestUtil.intToHash;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.transfer.files.ExportFileSupplier;
import org.projectnessie.versioned.transfer.files.ImportFileSupplier;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.HeadsAndForks;

public abstract class AbstractExportImport {

  @Test
  public void emptyExport(@TempDir Path targetDir) throws Exception {

    DatabaseAdapter databaseAdapter = mock(DatabaseAdapter.class);

    Instant now = Instant.now();
    DatabaseAdapterConfig config = mock(DatabaseAdapterConfig.class);
    when(config.getClock()).thenReturn(Clock.fixed(now, ZoneId.of("UTC")));
    when(databaseAdapter.getConfig()).thenReturn(config);

    when(databaseAdapter.namedRefs(any())).thenReturn(empty());
    when(databaseAdapter.scanAllCommitLogEntries()).thenReturn(empty());

    try (ExportFileSupplier exportFileSupplier = prepareExporter(targetDir)) {
      NessieExporter.builder()
          .exportFileSupplier(exportFileSupplier)
          .databaseAdapter(databaseAdapter)
          .fullScan(true)
          .build()
          .exportNessieRepository();
    }

    assertThat(readMeta(targetDir))
        .extracting(
            ExportMeta::getCommitCount,
            ExportMeta::getNamedReferencesCount,
            ExportMeta::getCommitsFilesCount,
            ExportMeta::getNamedReferencesFilesCount,
            ExportMeta::getVersion,
            ExportMeta::getCreatedMillisEpoch)
        .containsExactly(0L, 0L, 0, 0, ExportVersion.V1, now.toEpochMilli());

    List<String> fileNames = listFiles(targetDir);
    assertThat(fileNames).containsExactlyInAnyOrder(EXPORT_METADATA, HEADS_AND_FORKS);
  }

  @Test
  public void manyExportFiles(@TempDir Path targetDir) throws Exception {
    DatabaseAdapter exportDatabaseAdapter = mock(DatabaseAdapter.class);

    int keyListDistance = 20;
    Instant now = Instant.now();
    DatabaseAdapterConfig config = mock(DatabaseAdapterConfig.class);
    when(config.getClock()).thenReturn(Clock.fixed(now, ZoneId.of("UTC")));
    when(config.getKeyListDistance()).thenReturn(keyListDistance);
    when(exportDatabaseAdapter.getConfig()).thenReturn(config);

    int numNamedRefs = 20_000;
    int numCommits = 100_000;

    when(exportDatabaseAdapter.namedRefs(any()))
        .thenReturn(
            IntStream.rangeClosed(1, numNamedRefs)
                .mapToObj(i -> ReferenceInfo.of(intToHash(i), BranchName.of("branch-" + i))));

    when(exportDatabaseAdapter.scanAllCommitLogEntries())
        .thenReturn(
            IntStream.rangeClosed(1, numCommits).mapToObj(ExportImportTestUtil::toCommitLogEntry));

    try (ExportFileSupplier exportFileSupplier = prepareExporter(targetDir)) {
      NessieExporter.builder()
          .exportFileSupplier(exportFileSupplier)
          .databaseAdapter(exportDatabaseAdapter)
          .maxFileSize(128 * 1024)
          .fullScan(true)
          .build()
          .exportNessieRepository();
    }

    int expectedCommitsFileCount = 126;
    List<String> expectedCommitsFileNames =
        IntStream.rangeClosed(1, expectedCommitsFileCount)
            .mapToObj(i -> String.format("commits-%08d", i))
            .collect(Collectors.toList());

    int expectedNamedRefsFileCount = 3;
    List<String> expectedNamesRefsFileNames =
        IntStream.rangeClosed(1, expectedNamedRefsFileCount)
            .mapToObj(i -> String.format("named-refs-%08d", i))
            .collect(Collectors.toList());

    ExportMeta exportMeta = readMeta(targetDir);
    assertThat(exportMeta)
        .extracting(
            ExportMeta::getCommitCount,
            ExportMeta::getNamedReferencesCount,
            ExportMeta::getCommitsFilesCount,
            ExportMeta::getNamedReferencesFilesCount,
            ExportMeta::getVersion,
            ExportMeta::getCreatedMillisEpoch)
        .containsExactly(
            (long) numCommits,
            (long) numNamedRefs,
            expectedCommitsFileCount,
            expectedNamedRefsFileCount,
            ExportVersion.V1,
            now.toEpochMilli());
    assertThat(exportMeta.getCommitsFilesList())
        .containsExactlyElementsOf(expectedCommitsFileNames);
    assertThat(exportMeta.getNamedReferencesFilesList())
        .containsExactlyElementsOf(expectedNamesRefsFileNames);

    List<String> fileNames = listFiles(targetDir);
    assertThat(fileNames)
        .hasSize(2 + expectedNamedRefsFileCount + expectedCommitsFileCount)
        .contains(EXPORT_METADATA, HEADS_AND_FORKS)
        .containsAll(expectedNamesRefsFileNames)
        .containsAll(expectedCommitsFileNames);

    // Check that the importer code calls DatabaseAdapter.writeCommitUnconditional + .create()
    // with the correct arguments.
    // Using Mockito here for the amount of interactions at play is way too slow.

    BitSet createdReferences = new BitSet();
    BitSet createdCommits = new BitSet();

    DatabaseAdapter importDatabaseAdapter =
        (DatabaseAdapter)
            Proxy.newProxyInstance(
                Thread.currentThread().getContextClassLoader(),
                new Class[] {DatabaseAdapter.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "eraseRepo":
                    case "initializeRepo":
                    case "delete":
                      return null;
                    case "getConfig":
                      return config;
                    case "writeMultipleCommits":
                      {
                        @SuppressWarnings("unchecked")
                        List<CommitLogEntry> entries = (List<CommitLogEntry>) args[0];
                        entries.forEach(
                            logEntry -> {
                              int commit = parseInt(logEntry.getHash().asString(), 16);
                              assertThat(logEntry)
                                  .extracting(
                                      CommitLogEntry::getHash,
                                      CommitLogEntry::getParents,
                                      CommitLogEntry::getCommitSeq,
                                      CommitLogEntry::getKeyListDistance,
                                      CommitLogEntry::getCreatedTime,
                                      CommitLogEntry::getMetadata,
                                      CommitLogEntry::getDeletes,
                                      CommitLogEntry::getPuts)
                                  .containsExactly(
                                      intToHash(commit),
                                      Collections.singletonList(intToHash(commit - 1)),
                                      (long) commit,
                                      commit % keyListDistance,
                                      100L + commit,
                                      commitMeta(commit),
                                      emptyList(),
                                      emptyList());
                              assertThat(createdCommits.get(commit)).isFalse();
                              createdCommits.set(commit);
                            });
                        return null;
                      }
                    case "updateMultipleCommits":
                      fail();
                      return null;
                    case "create":
                      {
                        NamedRef ref = (NamedRef) args[0];
                        Hash hash = (Hash) args[1];
                        assertThat(ref.getName()).startsWith("branch-");
                        int refNum = parseInt(ref.getName().substring("branch-".length()));
                        assertThat(ref).isInstanceOf(BranchName.class);
                        assertThat(hash).isEqualTo(intToHash(refNum));
                        assertThat(createdReferences.get(refNum)).isFalse();
                        createdReferences.set(refNum);
                        return ImmutableReferenceCreatedResult.builder()
                            .namedRef(ref)
                            .hash(hash)
                            .build();
                      }
                    default:
                      throw new UnsupportedOperationException(method.toString());
                  }
                });

    try (ImportFileSupplier importFileSupplier = prepareImporter(targetDir)) {
      NessieImporter importer =
          NessieImporter.builder()
              .importFileSupplier(importFileSupplier)
              .databaseAdapter(importDatabaseAdapter)
              .build();
      importer.importNessieRepository();
    }

    assertThat(createdReferences.cardinality()).isEqualTo(numNamedRefs);
    assertThat(createdCommits.cardinality()).isEqualTo(numCommits);
  }

  protected abstract ExportMeta readMeta(Path targetDir) throws IOException;

  protected abstract HeadsAndForks readHeadsAndForks(Path targetDir) throws IOException;

  protected abstract List<String> listFiles(Path targetDir) throws IOException;

  protected abstract ExportFileSupplier prepareExporter(Path targetDir);

  protected abstract ImportFileSupplier prepareImporter(Path targetDir);
}

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
package org.projectnessie.quarkus.cli;

import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.quarkus.cli.ImportRepository.ERASE_BEFORE_IMPORT;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.NessieVersion;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.quarkus.cli.ExportRepository.Format;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith({NessieCliTestExtension.class, SoftAssertionsExtension.class})
public class ITExportImport {
  @InjectSoftAssertions private SoftAssertions soft;

  @Test
  public void invalidArgs(QuarkusMainLauncher launcher, @TempDir Path tempDir) throws Exception {
    LaunchResult result = launcher.launch("export");
    soft.assertThat(result.exitCode()).isEqualTo(2);
    soft.assertThat(result.getErrorOutput())
        .contains("Missing required option: '--path=<export-to>'");

    result =
        launcher.launch(
            "export",
            ExportRepository.OUTPUT_FORMAT,
            "foo",
            ExportRepository.PATH,
            tempDir.resolve("some-file.zip").toString());
    soft.assertThat(result.exitCode()).isEqualTo(2);
    soft.assertThat(result.getErrorOutput())
        .contains(
            "Invalid value for option '--output-format': expected one of [ZIP, DIRECTORY] (case-sensitive) but was 'foo'");

    Path existingZipFile = tempDir.resolve("existing-file.zip");
    Files.createFile(existingZipFile);

    result = launcher.launch("export", ExportRepository.PATH, existingZipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(1);
    soft.assertThat(result.getErrorOutput())
        .contains(
            String.format(
                "Export file %s already exists, please delete it first, if you want to overwrite it.",
                existingZipFile));

    result =
        launcher.launch(
            "export",
            ExportRepository.PATH,
            existingZipFile.toString(),
            ExportRepository.OUTPUT_FORMAT,
            Format.DIRECTORY.toString());
    soft.assertThat(result.exitCode()).isEqualTo(1);
    soft.assertThat(result.getErrorOutput())
        .contains(
            String.format(
                "%s refers to a file, but export type is %s.", existingZipFile, Format.DIRECTORY));

    result = launcher.launch("import");
    soft.assertThat(result.exitCode()).isEqualTo(2);
    soft.assertThat(result.getErrorOutput())
        .contains("Missing required option: '--path=<import-from>'");

    result =
        launcher.launch("import", ImportRepository.PATH, tempDir.resolve("no-no.zip").toString());
    soft.assertThat(result.exitCode()).isEqualTo(1);
    soft.assertThat(result.getErrorOutput())
        .contains("No such file or directory " + tempDir.resolve("no-no.zip"));

    result = launcher.launch("import", ImportRepository.PATH, tempDir.resolve("no-no").toString());
    soft.assertThat(result.exitCode()).isEqualTo(1);
    soft.assertThat(result.getErrorOutput())
        .contains("No such file or directory " + tempDir.resolve("no-no"));
  }

  @Test
  public void emptyRepoExportToZip(QuarkusMainLauncher launcher, @TempDir Path tempDir) {
    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 0 commits into 0 files, 1 named references into 1 files.");
    soft.assertThat(zipFile).isRegularFile();

    // Importing into an "empty" repository passes the "empty-repository-check" during import
    result = launcher.launch("import", ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains("Imported Nessie repository, 0 commits, 1 named references.");
  }

  @Test
  public void emptyRepoExportToDir(QuarkusMainLauncher launcher, @TempDir Path tempDir) {
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 0 commits into 0 files, 1 named references into 1 files.");
    soft.assertThat(tempDir).isNotEmptyDirectory();

    // Importing into an "empty" repository passes the "empty-repository-check" during import
    result = launcher.launch("import", ImportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains("Imported Nessie repository, 0 commits, 1 named references.");
  }

  @Test
  public void nonEmptyRepoExportToZip(
      QuarkusMainLauncher launcher, DatabaseAdapter adapter, @TempDir Path tempDir)
      throws Exception {
    populateRepository(adapter);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 2 commits into 1 files, 2 named references into 1 files.");
    soft.assertThat(zipFile).isRegularFile();

    // Importing into a "non-empty" repository does not pass the "empty-repository-check"
    result = launcher.launch("import", ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(100);
    soft.assertThat(result.getErrorOutput())
        .contains(
            "The Nessie repository already exists and is not empty, aborting. "
                + "Provide the "
                + ERASE_BEFORE_IMPORT
                + " option if you want to erase the repository.");

    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains("Export was created by Nessie version " + NessieVersion.NESSIE_VERSION + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\) and [0-9]+ commits \\(in [0-9]+ files\\)")
        .contains("Imported Nessie repository, 2 commits, 2 named references.")
        .contains("Finished commit log optimization.");

    checkValues(
        adapter,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta", 42L, 43, 44, 45));
    checkValues(
        adapter,
        "branch-foo",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta2", 43L, 43, 44, 45));
  }

  @Test
  public void nonEmptyRepoExportToDir(
      QuarkusMainLauncher launcher, DatabaseAdapter adapter, @TempDir Path tempDir)
      throws Exception {
    populateRepository(adapter);

    LaunchResult result = launcher.launch("export", ExportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 2 commits into 1 files, 2 named references into 1 files.");
    soft.assertThat(tempDir).isNotEmptyDirectory();

    // Importing into a "non-empty" repository does not pass the "empty-repository-check"
    result = launcher.launch("import", ImportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(100);
    soft.assertThat(result.getErrorOutput())
        .contains(
            "The Nessie repository already exists and is not empty, aborting. "
                + "Provide the "
                + ERASE_BEFORE_IMPORT
                + " option if you want to erase the repository.");

    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains("Export was created by Nessie version " + NessieVersion.NESSIE_VERSION + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\) and [0-9]+ commits \\(in [0-9]+ files\\)")
        .contains("Imported Nessie repository, 2 commits, 2 named references.")
        .contains("Finished commit log optimization.");

    checkValues(
        adapter,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta", 42L, 43, 44, 45));
    checkValues(
        adapter,
        "branch-foo",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta2", 43L, 43, 44, 45));
  }

  @Test
  public void onlyContentExportToZip(
      QuarkusMainLauncher launcher, DatabaseAdapter adapter, @TempDir Path tempDir)
      throws Exception {
    populateRepository(adapter);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result =
        launcher.launch(
            "export",
            ExportRepository.SINGLE_BRANCH,
            "main",
            ExportRepository.CONTENT_BATCH_SIZE,
            "10",
            ExportRepository.PATH,
            zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 1 commits into 1 files, 1 named references into 1 files.");
    soft.assertThat(zipFile).isRegularFile();

    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains("Export was created by Nessie version " + NessieVersion.NESSIE_VERSION + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\) and [0-9]+ commits \\(in [0-9]+ files\\)")
        .contains("Imported Nessie repository, 1 commits, 1 named references.")
        .contains("Finished commit log optimization.");

    checkValues(
        adapter,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta", 42L, 43, 44, 45));
  }

  @Test
  public void testExportImportMergeCommit(
      QuarkusMainLauncher launcher, DatabaseAdapter adapter, @TempDir Path tempDir)
      throws Exception {

    populateRepositoryWithMergeCommit(adapter);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 4 commits into 1 files, 2 named references into 1 files.");
    soft.assertThat(zipFile).isRegularFile();

    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains("Export was created by Nessie version " + NessieVersion.NESSIE_VERSION + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\) and [0-9]+ commits \\(in [0-9]+ files\\)")
        .contains("Imported Nessie repository, 4 commits, 2 named references.")
        .contains("Import finalization finished, total duration: ");

    checkValues(
        adapter,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta3", 44, 43, 44, 45, tableId));
  }

  private void checkValues(
      DatabaseAdapter adapter, String ref, ContentKey key, IcebergTable expected)
      throws ReferenceNotFoundException {
    ReferenceInfo<ByteString> main = adapter.namedRef(ref, GetNamedRefsParams.DEFAULT);
    Map<ContentKey, ContentAndState> values =
        adapter.values(
            main.getHash(), Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL);
    soft.assertThat(values).containsKey(key);
    soft.assertThat(
            DefaultStoreWorker.instance()
                .valueFromStore(values.get(key).getPayload(), values.get(key).getRefState()))
        .asInstanceOf(type(IcebergTable.class))
        .extracting(
            IcebergTable::getMetadataLocation,
            IcebergTable::getSnapshotId,
            IcebergTable::getSchemaId,
            IcebergTable::getSpecId,
            IcebergTable::getSortOrderId)
        .containsExactly(
            expected.getMetadataLocation(),
            expected.getSnapshotId(),
            expected.getSchemaId(),
            expected.getSpecId(),
            expected.getSortOrderId());
  }

  static String namespaceId = UUID.randomUUID().toString();
  static String tableId = UUID.randomUUID().toString();

  private static void populateRepository(DatabaseAdapter adapter)
      throws ReferenceConflictException,
          ReferenceNotFoundException,
          ReferenceAlreadyExistsException {
    BranchName branchMain = BranchName.of("main");
    BranchName branchFoo = BranchName.of("branch-foo");

    ByteString commitMeta =
        CommitMetaSerializer.METADATA_SERIALIZER.toBytes(CommitMeta.fromMessage("hello"));
    ContentKey key = ContentKey.of("namespace123", "table123");
    CommitResult<CommitLogEntry> main =
        adapter.commit(
            ImmutableCommitParams.builder()
                .toBranch(branchMain)
                .commitMetaSerialized(commitMeta)
                .addPuts(
                    KeyWithBytes.of(
                        key.getParent(),
                        ContentId.of(namespaceId),
                        (byte) payloadForContent(NAMESPACE),
                        DefaultStoreWorker.instance()
                            .toStoreOnReferenceState(
                                Namespace.builder()
                                    .id(namespaceId)
                                    .addElements("namespace123")
                                    .build())),
                    KeyWithBytes.of(
                        key,
                        ContentId.of(tableId),
                        (byte) payloadForContent(ICEBERG_TABLE),
                        DefaultStoreWorker.instance()
                            .toStoreOnReferenceState(
                                IcebergTable.of("meta", 42, 43, 44, 45, tableId))))
                .build());
    adapter.create(branchFoo, main.getCommitHash());
    adapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(branchFoo)
            .commitMetaSerialized(commitMeta)
            .addPuts(
                KeyWithBytes.of(
                    key,
                    ContentId.of(tableId),
                    (byte) payloadForContent(ICEBERG_TABLE),
                    DefaultStoreWorker.instance()
                        .toStoreOnReferenceState(
                            IcebergTable.of("meta2", 43, 43, 44, 45, tableId))))
            .build());
  }

  private void populateRepositoryWithMergeCommit(DatabaseAdapter adapter) throws Exception {
    populateRepository(adapter);

    BranchName branchMain = BranchName.of("main");
    BranchName branchTemp = BranchName.of("temp");

    Hash mainHead = adapter.namedRef("main", GetNamedRefsParams.DEFAULT).getHash();
    adapter.create(branchTemp, mainHead);

    ContentKey key = ContentKey.of("namespace123", "table123");
    CommitResult<CommitLogEntry> temp =
        adapter.commit(
            ImmutableCommitParams.builder()
                .toBranch(branchTemp)
                .commitMetaSerialized(
                    CommitMetaSerializer.METADATA_SERIALIZER.toBytes(
                        CommitMeta.fromMessage("hello from temp")))
                .addPuts(
                    KeyWithBytes.of(
                        key.getParent(),
                        ContentId.of(namespaceId),
                        (byte) payloadForContent(NAMESPACE),
                        DefaultStoreWorker.instance()
                            .toStoreOnReferenceState(
                                Namespace.builder()
                                    .id(namespaceId)
                                    .addElements("namespace123")
                                    .build())),
                    KeyWithBytes.of(
                        key,
                        ContentId.of(tableId),
                        (byte) payloadForContent(ICEBERG_TABLE),
                        DefaultStoreWorker.instance()
                            .toStoreOnReferenceState(
                                IcebergTable.of("meta3", 44, 43, 44, 45, tableId))))
                .build());

    adapter.merge(
        MergeParams.builder()
            .mergeFromHash(temp.getCommitHash())
            .fromRef(branchTemp)
            .toBranch(branchMain)
            .keepIndividualCommits(false)
            .updateCommitMetadata(
                new MetadataRewriter<>() {
                  @Override
                  public ByteString rewriteSingle(ByteString metadata) {
                    return ByteString.copyFromUtf8(metadata.toStringUtf8() + " merged");
                  }

                  @Override
                  public ByteString squash(List<ByteString> metadata, int numCommits) {
                    return rewriteSingle(metadata.get(0));
                  }
                })
            .build());

    // Delete the temp branch so that the secondary parent won't be directly reachable anymore.
    adapter.delete(branchTemp, Optional.empty());
  }
}

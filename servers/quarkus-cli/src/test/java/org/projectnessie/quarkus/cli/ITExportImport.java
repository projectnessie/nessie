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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.quarkus.cli.ExportRepository.TARGET_DIRECTORY;
import static org.projectnessie.quarkus.cli.ImportRepository.ERASE_BEFORE_IMPORT;
import static org.projectnessie.quarkus.cli.ImportRepository.SOURCE_DIRECTORY;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.google.protobuf.ByteString;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitMetaSerializer;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@QuarkusMainTest
@TestProfile(QuarkusCliTestProfileMongo.class)
@ExtendWith(NessieCliTestExtension.class)
public class ITExportImport {

  @Test
  public void invalidArgs(QuarkusMainLauncher launcher, @TempDir Path tempDir) {
    Path zipFile = tempDir.resolve("export.zip");

    LaunchResult result = launcher.launch("export");
    assertThat(result.exitCode()).isEqualTo(2);
    assertThat(result.getErrorOutput())
        .contains(
            "Error: Missing required argument (specify one of these): (-f=<zipFile> | -d=<targetDirectory>)");

    result =
        launcher.launch(
            "export",
            ExportRepository.ZIP_FILE,
            zipFile.toString(),
            TARGET_DIRECTORY,
            tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(2);
    assertThat(result.getErrorOutput())
        .contains(
            "Error: --zip-file=<zipFile>, --target-directory=<targetDirectory> are mutually exclusive (specify only one)");

    result = launcher.launch("import");
    assertThat(result.exitCode()).isEqualTo(2);
    assertThat(result.getErrorOutput())
        .contains(
            "Error: Missing required argument (specify one of these): (-f=<zipFile> | -d=<sourceDirectory>)");

    result =
        launcher.launch(
            "import",
            ImportRepository.ZIP_FILE,
            zipFile.toString(),
            SOURCE_DIRECTORY,
            tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(2);
    assertThat(result.getErrorOutput())
        .contains(
            "Error: --zip-file=<zipFile>, --source-directory=<sourceDirectory> are mutually exclusive (specify only one)");

    result =
        launcher.launch(
            "import", ImportRepository.ZIP_FILE, tempDir.resolve("no-no.zip").toString());
    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.getErrorOutput())
        .contains("java.nio.file.NoSuchFileException: " + tempDir.resolve("no-no.zip"));

    result = launcher.launch("import", SOURCE_DIRECTORY, tempDir.resolve("no-no").toString());
    assertThat(result.exitCode()).isEqualTo(1);
    assertThat(result.getErrorOutput())
        .contains("java.nio.file.NoSuchFileException: " + tempDir.resolve("no-no"));
  }

  @Test
  public void emptyRepoExportToZip(QuarkusMainLauncher launcher, @TempDir Path tempDir) {
    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.ZIP_FILE, zipFile.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 0 commits into 0 files, 1 named references into 1 files.");
    assertThat(zipFile).isRegularFile();

    // Importing into an "empty" repository passes the "empty-repository-check" during import
    result = launcher.launch("import", ImportRepository.ZIP_FILE, zipFile.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Imported Nessie repository, 0 commits, 1 named references.");
  }

  @Test
  public void emptyRepoExportToDir(QuarkusMainLauncher launcher, @TempDir Path tempDir) {
    LaunchResult result = launcher.launch("export", TARGET_DIRECTORY, tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 0 commits into 0 files, 1 named references into 1 files.");
    assertThat(tempDir).isNotEmptyDirectory();

    // Importing into an "empty" repository passes the "empty-repository-check" during import
    result = launcher.launch("import", SOURCE_DIRECTORY, tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Imported Nessie repository, 0 commits, 1 named references.");
  }

  @Test
  public void nonEmptyRepoExportToZip(
      QuarkusMainLauncher launcher, DatabaseAdapter adapter, @TempDir Path tempDir)
      throws Exception {
    populateRepository(adapter);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.ZIP_FILE, zipFile.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 2 commits into 1 files, 2 named references into 1 files.");
    assertThat(zipFile).isRegularFile();

    // Importing into a "non-empty" repository does not pass the "empty-repository-check"
    result = launcher.launch("import", ImportRepository.ZIP_FILE, zipFile.toString());
    assertThat(result.exitCode()).isEqualTo(100);
    assertThat(result.getErrorOutput())
        .contains(
            "The Nessie repository already exists and is not empty, aborting. "
                + "Provide the "
                + ERASE_BEFORE_IMPORT
                + " option if you want to erase the repository.");

    result =
        launcher.launch(
            "import", ERASE_BEFORE_IMPORT, ImportRepository.ZIP_FILE, zipFile.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Imported Nessie repository, 2 commits, 2 named references.")
        .contains("Finished commit log optimization.");
  }

  @Test
  public void nonEmptyRepoExportToDir(
      QuarkusMainLauncher launcher, DatabaseAdapter adapter, @TempDir Path tempDir)
      throws Exception {
    populateRepository(adapter);

    LaunchResult result = launcher.launch("export", TARGET_DIRECTORY, tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains(
            "Exported Nessie repository, 2 commits into 1 files, 2 named references into 1 files.");
    assertThat(tempDir).isNotEmptyDirectory();

    // Importing into a "non-empty" repository does not pass the "empty-repository-check"
    result = launcher.launch("import", SOURCE_DIRECTORY, tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(100);
    assertThat(result.getErrorOutput())
        .contains(
            "The Nessie repository already exists and is not empty, aborting. "
                + "Provide the "
                + ERASE_BEFORE_IMPORT
                + " option if you want to erase the repository.");

    result = launcher.launch("import", ERASE_BEFORE_IMPORT, SOURCE_DIRECTORY, tempDir.toString());
    assertThat(result.exitCode()).isEqualTo(0);
    assertThat(result.getOutput())
        .contains("Imported Nessie repository, 2 commits, 2 named references.")
        .contains("Finished commit log optimization.");
  }

  private static void populateRepository(DatabaseAdapter adapter)
      throws ReferenceConflictException, ReferenceNotFoundException,
          ReferenceAlreadyExistsException {
    BranchName branchMain = BranchName.of("main");
    BranchName branchFoo = BranchName.of("branch-foo");

    ByteString commitMeta =
        CommitMetaSerializer.METADATA_SERIALIZER.toBytes(CommitMeta.fromMessage("hello"));
    Key key = Key.of("namespace123", "table123");
    Hash main =
        adapter.commit(
            ImmutableCommitParams.builder()
                .toBranch(branchMain)
                .commitMetaSerialized(commitMeta)
                .addPuts(
                    KeyWithBytes.of(
                        key,
                        ContentId.of("id123"),
                        payloadForContent(ICEBERG_TABLE),
                        DefaultStoreWorker.instance()
                            .toStoreOnReferenceState(
                                IcebergTable.of("meta", 42, 43, 44, 45, "id123"), a -> {})))
                .build());
    adapter.create(branchFoo, main);
    adapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(branchFoo)
            .commitMetaSerialized(commitMeta)
            .addPuts(
                KeyWithBytes.of(
                    key,
                    ContentId.of("id123"),
                    payloadForContent(ICEBERG_TABLE),
                    DefaultStoreWorker.instance()
                        .toStoreOnReferenceState(
                            IcebergTable.of("meta2", 43, 43, 44, 45, "id123"), a -> {})))
            .build());
  }
}

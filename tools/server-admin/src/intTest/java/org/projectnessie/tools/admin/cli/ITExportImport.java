/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.tools.admin.cli;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.tools.admin.cli.ImportRepository.ERASE_BEFORE_IMPORT;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.newCommitBuilder;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.referenceLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.UNIQUE;
import static org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj.uniqueId;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKey;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainLauncher;
import io.quarkus.test.junit.main.QuarkusMainTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.NessieVersion;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.quarkus.tests.profiles.BaseConfigProfile;
import org.projectnessie.tools.admin.cli.ExportRepository.Format;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.CommitLogic;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.UniqueIdObj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.store.DefaultStoreWorker;

@QuarkusMainTest
@TestProfile(BaseConfigProfile.class)
@ExtendWith({NessieServerAdminTestExtension.class, SoftAssertionsExtension.class})
public class ITExportImport {

  private static final String VERSION_STORES =
      "(ROCKSDB|DYNAMODB2|MONGODB2|CASSANDRA2|JDBC2|BIGTABLE)";

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
        .containsPattern(
            "Exporting from a " + VERSION_STORES + " version store using export version 3...")
        .contains(
            "Exported Nessie repository, 0 commits into 0 files, 1 named references into 1 files, 0 generic objects into 0 files.");
    soft.assertThat(zipFile).isRegularFile();

    // Importing into an "empty" repository passes the "empty-repository-check" during import
    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern("Importing into a " + VERSION_STORES + " version store...")
        .contains("Imported Nessie repository, 0 commits, 1 named references, 0 generic objects.");
  }

  @Test
  public void emptyRepoExportToDir(QuarkusMainLauncher launcher, @TempDir Path tempDir) {
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern(
            "Exporting from a " + VERSION_STORES + " version store using export version 3...")
        .contains(
            "Exported Nessie repository, 0 commits into 0 files, 1 named references into 1 files, 0 generic objects into 0 files.");
    soft.assertThat(tempDir).isNotEmptyDirectory();

    // Importing into an "empty" repository passes the "empty-repository-check" during import
    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern("Importing into a " + VERSION_STORES + " version store...")
        .contains("Imported Nessie repository, 0 commits, 1 named references, 0 generic objects.");
  }

  @Test
  public void nonEmptyRepoExportToZip(
      QuarkusMainLauncher launcher, Persist persist, @TempDir Path tempDir) throws Exception {
    populateRepository(persist);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern(
            "Exporting from a " + VERSION_STORES + " version store using export version 3...")
        .contains(
            "Exported Nessie repository, 2 commits into 1 files, 2 named references into 1 files, 1 generic objects into 1 files.");
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
        .contains(
            "Export using format V3 was created by Nessie version "
                + NessieVersion.NESSIE_VERSION
                + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\), [0-9]+ commits \\(in [0-9]+ files\\), [0-9]+ generic objects \\(in [0-9]+ files\\)")
        .containsPattern("Importing into a " + VERSION_STORES + " version store...")
        .contains("Imported Nessie repository, 2 commits, 2 named references, 1 generic objects.")
        .contains("Import finalization finished, total duration: ");

    checkValues(
        persist,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta", 42, 43, 44, 45, contentIdStr));
    checkValues(
        persist,
        "branch-foo",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta2", 43, 43, 44, 45, contentIdStr));
  }

  @Test
  public void nonEmptyRepoExportToDir(
      QuarkusMainLauncher launcher, Persist persist, @TempDir Path tempDir) throws Exception {
    populateRepository(persist);

    LaunchResult result = launcher.launch("export", ExportRepository.PATH, tempDir.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern(
            "Exporting from a " + VERSION_STORES + " version store using export version 3...")
        .contains(
            "Exported Nessie repository, 2 commits into 1 files, 2 named references into 1 files, 1 generic objects into 1 files.");
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
        .contains(
            "Export using format V3 was created by Nessie version "
                + NessieVersion.NESSIE_VERSION
                + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\), [0-9]+ commits \\(in [0-9]+ files\\), [0-9]+ generic objects \\(in [0-9]+ files\\)")
        .containsPattern("Importing into a " + VERSION_STORES + " version store...")
        .contains("Imported Nessie repository, 2 commits, 2 named references, 1 generic objects.")
        .contains("Import finalization finished, total duration: ");

    checkValues(
        persist,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta", 42, 43, 44, 45, contentIdStr));
    checkValues(
        persist,
        "branch-foo",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta2", 43, 43, 44, 45, contentIdStr));
  }

  @Test
  public void onlyContentExportToZip(
      QuarkusMainLauncher launcher, Persist persist, @TempDir Path tempDir) throws Exception {
    populateRepository(persist);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result =
        launcher.launch(
            "export",
            ExportRepository.SINGLE_BRANCH,
            "main",
            ExportRepository.PATH,
            zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern(
            "Exporting from a " + VERSION_STORES + " version store using export version 3...")
        .contains(
            "Exported Nessie repository, 1 commits into 1 files, 1 named references into 1 files, 1 generic objects into 1 files.");
    soft.assertThat(zipFile).isRegularFile();

    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Export using format V3 was created by Nessie version "
                + NessieVersion.NESSIE_VERSION
                + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\), [0-9]+ commits \\(in [0-9]+ files\\), [0-9]+ generic objects \\(in [0-9]+ files\\)")
        .containsPattern("Importing into a " + VERSION_STORES + " version store...")
        .contains("Imported Nessie repository, 1 commits, 1 named references, 1 generic objects.")
        .contains("Import finalization finished, total duration: ");

    checkValues(
        persist,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta", 42, 43, 44, 45, contentIdStr));
  }

  @Test
  public void testExportImportMergeCommit(
      QuarkusMainLauncher launcher, Persist persist, @TempDir Path tempDir) throws Exception {

    populateRepositoryWithMergeCommit(persist);

    Path zipFile = tempDir.resolve("export.zip");
    LaunchResult result = launcher.launch("export", ExportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .containsPattern(
            "Exporting from a " + VERSION_STORES + " version store using export version 3...")
        .contains(
            "Exported Nessie repository, 4 commits into 1 files, 2 named references into 1 files, 1 generic objects into 1 files.");
    soft.assertThat(zipFile).isRegularFile();

    result =
        launcher.launch("import", ERASE_BEFORE_IMPORT, ImportRepository.PATH, zipFile.toString());
    soft.assertThat(result.exitCode()).isEqualTo(0);
    soft.assertThat(result.getOutput())
        .contains(
            "Export using format V3 was created by Nessie version "
                + NessieVersion.NESSIE_VERSION
                + " on ")
        .containsPattern(
            "containing [0-9]+ named references \\(in [0-9]+ files\\), [0-9]+ commits \\(in [0-9]+ files\\), [0-9]+ generic objects \\(in [0-9]+ files\\)")
        .containsPattern("Importing into a " + VERSION_STORES + " version store...")
        .contains("Imported Nessie repository, 4 commits, 2 named references, 1 generic objects.")
        .contains("Import finalization finished, total duration: ");

    checkValues(
        persist,
        "main",
        ContentKey.of("namespace123", "table123"),
        IcebergTable.of("meta3", 44, 43, 44, 45, contentIdStr));
  }

  private void checkValues(Persist persist, String ref, ContentKey key, Content value)
      throws ReferenceNotFoundException {
    VersionStoreImpl store = new VersionStoreImpl(persist);
    ReferenceInfo<CommitMeta> main = store.getNamedRef(ref, GetNamedRefsParams.DEFAULT);
    soft.assertThat(store.getKeys(main.getHash(), null, true, NO_KEY_RESTRICTIONS))
        .toIterable()
        .extracting(e -> e.getKey().contentKey(), KeyEntry::getContent)
        .containsExactly(tuple(key, value));

    UniqueIdObj uniqueId = uniqueId("content-id", contentId);
    soft.assertThatCode(
            () ->
                soft.assertThat(
                        persist.fetchTypedObj(
                            requireNonNull(uniqueId.id()), UNIQUE, UniqueIdObj.class))
                    .isEqualTo(uniqueId))
        .doesNotThrowAnyException();
  }

  private final UUID contentId = UUID.randomUUID();
  private final String contentIdStr = contentId.toString();

  private void populateRepository(Persist persist) throws Exception {
    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);

    Reference refMain = referenceLogic.getReference("refs/heads/main");

    StoreWorker storeWorker = DefaultStoreWorker.instance();
    int payload = payloadForContent(ICEBERG_TABLE);
    ByteString contentMain =
        storeWorker.toStoreOnReferenceState(IcebergTable.of("meta", 42, 43, 44, 45, contentIdStr));
    ByteString contentFoo =
        storeWorker.toStoreOnReferenceState(IcebergTable.of("meta2", 43, 43, 44, 45, contentIdStr));

    ContentValueObj valueMain = contentValue(contentIdStr, payload, contentMain);
    ContentValueObj valueFoo = contentValue(contentIdStr, payload, contentFoo);

    soft.assertThat(persist.storeObj(valueMain)).isTrue();
    StoreKey key = keyToStoreKey(ContentKey.of("namespace123", "table123"));
    CommitObj main =
        commitLogic.doCommit(
            newCommitBuilder()
                .parentCommitId(EMPTY_OBJ_ID)
                .addAdds(commitAdd(key, payload, requireNonNull(valueMain.id()), null, contentId))
                .message("hello commit on main")
                .headers(EMPTY_COMMIT_HEADERS)
                .build(),
            emptyList());
    referenceLogic.assignReference(refMain, requireNonNull(main).id());

    UniqueIdObj uniqueId = uniqueId("content-id", contentId);
    soft.assertThat(persist.storeObj(uniqueId)).isTrue();

    Reference refFoo =
        referenceLogic.createReference("refs/heads/branch-foo", main.id(), randomObjId());
    soft.assertThat(persist.storeObj(valueFoo)).isTrue();
    CommitObj foo =
        commitLogic.doCommit(
            newCommitBuilder()
                .parentCommitId(main.id())
                .addAdds(
                    commitAdd(
                        key,
                        payload,
                        requireNonNull(valueFoo.id()),
                        requireNonNull(valueMain.id()),
                        contentId))
                .message("hello commit on foo")
                .headers(EMPTY_COMMIT_HEADERS)
                .build(),
            emptyList());
    referenceLogic.assignReference(refFoo, requireNonNull(foo).id());
  }

  private void populateRepositoryWithMergeCommit(Persist persist) throws Exception {
    populateRepository(persist);
    ReferenceLogic referenceLogic = referenceLogic(persist);
    CommitLogic commitLogic = commitLogic(persist);
    Reference refMain = referenceLogic.getReference("refs/heads/main");

    StoreWorker storeWorker = DefaultStoreWorker.instance();
    int payload = payloadForContent(ICEBERG_TABLE);
    ByteString contentTemp =
        storeWorker.toStoreOnReferenceState(IcebergTable.of("meta3", 44, 43, 44, 45, contentIdStr));
    ByteString contentMain =
        storeWorker.toStoreOnReferenceState(IcebergTable.of("meta", 42, 43, 44, 45, contentIdStr));

    ContentValueObj valueMain = contentValue(contentIdStr, payload, contentMain);
    ContentValueObj valueTemp = contentValue(contentIdStr, payload, contentTemp);

    soft.assertThat(persist.storeObj(valueTemp)).isTrue();

    StoreKey key = keyToStoreKey(ContentKey.of("namespace123", "table123"));

    CommitObj temp =
        commitLogic.doCommit(
            newCommitBuilder()
                .parentCommitId(refMain.pointer())
                .addAdds(
                    commitAdd(
                        key,
                        payload,
                        requireNonNull(valueTemp.id()),
                        requireNonNull(valueMain.id()),
                        contentId))
                .message("hello commit on temp")
                .headers(EMPTY_COMMIT_HEADERS)
                .build(),
            emptyList());

    CommitObj merge =
        commitLogic.doCommit(
            newCommitBuilder()
                .parentCommitId(refMain.pointer())
                .addSecondaryParents(requireNonNull(temp).id())
                .addAdds(
                    commitAdd(
                        key,
                        payload,
                        requireNonNull(valueTemp.id()),
                        requireNonNull(valueMain.id()),
                        contentId))
                .message("merge commit from temp into main")
                .headers(EMPTY_COMMIT_HEADERS)
                .build(),
            emptyList());

    referenceLogic.assignReference(refMain, requireNonNull(merge).id());
  }
}

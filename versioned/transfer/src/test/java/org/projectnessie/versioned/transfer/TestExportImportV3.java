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

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_REPOSITORY_ID;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.ImmutableGenericObj;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypes;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackendName;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.FileImporter;
import org.projectnessie.versioned.transfer.files.ZipArchiveExporter;
import org.projectnessie.versioned.transfer.files.ZipArchiveImporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;

@ExtendWith(PersistExtension.class)
@NessieBackendName(InmemoryBackendFactory.NAME)
public class TestExportImportV3 extends BaseExportImport {

  @NessiePersist
  @NessieStoreConfig(name = CONFIG_REPOSITORY_ID, value = "import-target")
  protected static Persist persistImport;

  @NessiePersist
  @NessieStoreConfig(name = CONFIG_REPOSITORY_ID, value = "export-source")
  protected static Persist persistExport;

  @Override
  ExportVersion exportVersion() {
    return ExportVersion.V3;
  }

  @Override
  Persist sourcePersist() {
    return persistExport;
  }

  @Override
  Persist targetPersist() {
    return persistImport;
  }

  @Override
  VersionStore sourceVersionStore() {
    return new VersionStoreImpl(persistExport);
  }

  @Override
  VersionStore targetVersionStore() {
    return new VersionStoreImpl(persistImport);
  }

  @Override
  void prepareTargetRepo() {
    persistImport.erase();
    // Don't initialize the repository, since the import does that.
  }

  @Override
  ImportResult importRepo(boolean zip) throws IOException {
    NessieImporter importer =
        NessieImporter.builder()
            .persist(persistImport)
            .importFileSupplier(
                zip
                    ? ZipArchiveImporter.builder().sourceZipFile(dir.resolve("export.zip")).build()
                    : FileImporter.builder().sourceDirectory(dir).build())
            .build();
    return importer.importNessieRepository();
  }

  @Override
  ExportMeta exportRepo(boolean zip, boolean fullScan) throws Exception {
    NessieExporter exporter =
        NessieExporter.builder()
            .persist(persistExport)
            .fullScan(fullScan)
            .exportFileSupplier(
                zip
                    ? ZipArchiveExporter.builder().outputFile(dir.resolve("export.zip")).build()
                    : FileExporter.builder().targetDirectory(dir).build())
            .build();
    try {
      return exporter.exportNessieRepository();
    } finally {
      exporter.exportFileSupplier().close();
    }
  }

  @Override
  Stream<Hash> scanAllTargetCommits() {
    CloseableIterator<Obj> iter = persistImport.scanAllObjects(Set.of(StandardObjType.COMMIT));
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false)
        .onClose(iter::close)
        .map(CommitObj.class::cast)
        .filter(o -> o.commitType() != CommitType.INTERNAL)
        .map(o -> Hash.of(o.id().asBytes()));
  }

  @Override
  protected void checkRepositoryDescription() {
    RepositoryDescription description =
        requireNonNull(repositoryLogic(persistImport).fetchRepositoryDescription());
    soft.assertThat(description.defaultBranchName()).isEqualTo("main");
    soft.assertThat(description.repositoryCreatedTime()).isNotNull();
    soft.assertThat(description.oldestPossibleCommitTime()).isNotNull();
    soft.assertThat(description.repositoryImportedTime()).isNotNull();
  }

  static Stream<Arguments> relatedObjectsJarWorks() {
    return Stream.of(
        arguments(true, null, 2L, 5L, true),
        arguments(true, null, 2L, 1L, false),
        //
        arguments(false, null, 2L, 5L, true),
        arguments(false, null, 2L, 1L, false),
        // export only contents from HEAD of branch 'branch'
        arguments(false, "branch", 1L, 5L, true),
        arguments(false, "branch", 1L, 1L, false)
        //
        );
  }

  /**
   * Uses {@code org.projectnessie.versioned.transfer.testing.RelatedObjectsForTesting} via a jar
   * supplied as a parameter, verifying the {@link NessieExporter#genericObjectResolvers()}
   * functionality.
   */
  @ParameterizedTest
  @MethodSource
  void relatedObjectsJarWorks(
      boolean fullScan, String sourceBranch, long numRefs, long numGeneric, boolean addJar)
      throws Exception {

    ObjId repoObjId = ObjId.objIdFromString("deadbeef");
    ObjId commitObjId = ObjId.objIdFromString("cafebabe");
    ObjId contentObjId = ObjId.objIdFromString("f00dfeed");
    ObjId referenceObjId = ObjId.objIdFromString("1dea5eed");

    ObjType repoObjType = ObjTypes.objTypeByName("repo-generic");
    ObjType commitObjType = ObjTypes.objTypeByName("commit-generic");
    ObjType contentObjType = ObjTypes.objTypeByName("content-generic");
    ObjType referenceObjType = ObjTypes.objTypeByName("reference-generic");

    Obj repoObj =
        ImmutableGenericObj.builder()
            .type(repoObjType)
            .id(repoObjId)
            .putAttributes("hello", "repo")
            .build();
    Obj commitObj =
        ImmutableGenericObj.builder()
            .type(commitObjType)
            .id(commitObjId)
            .putAttributes("hello", "commit")
            .build();
    Obj contentObj =
        ImmutableGenericObj.builder()
            .type(contentObjType)
            .id(contentObjId)
            .putAttributes("hello", "content")
            .build();
    Obj referenceObj =
        ImmutableGenericObj.builder()
            .type(referenceObjType)
            .id(referenceObjId)
            .putAttributes("hello", "reference")
            .build();

    soft.assertThat(
            sourcePersist().storeObjs(new Obj[] {repoObj, commitObj, contentObj, referenceObj}))
        .containsExactly(true, true, true, true);

    VersionStore vs = sourceVersionStore();
    BranchName branchName = BranchName.of("branch");
    ReferenceCreatedResult createdRef = vs.create(branchName, Optional.of(vs.noAncestorHash()));
    vs.commit(
            branchName,
            Optional.of(createdRef.getHash()),
            CommitMeta.fromMessage("commit"),
            Collections.singletonList(
                Put.of(ContentKey.of("my-table"), IcebergTable.of("meta", 42, 43, 44, 45))))
        .getCommitHash();

    URL relatedObjectsJar = Paths.get(System.getProperty("related-objects-jar")).toUri().toURL();

    NessieExporter.Builder exporter =
        NessieExporter.builder()
            .persist(persistExport)
            .fullScan(fullScan)
            .contentsFromBranch(sourceBranch)
            .versionStore(sourceVersionStore())
            .exportFileSupplier(FileExporter.builder().targetDirectory(dir).build());
    if (addJar) {
      exporter.addGenericObjectResolvers(relatedObjectsJar);
    }

    ExportMeta result = exporter.build().exportNessieRepository();

    soft.assertThat(result)
        .extracting(
            ExportMeta::getCommitCount,
            ExportMeta::getNamedReferencesCount,
            ExportMeta::getGenericObjCount)
        .containsExactly(1L, numRefs, numGeneric);

    prepareTargetRepo();

    ImportResult importResult = importRepo(false);

    soft.assertThat(importResult)
        .extracting(
            ImportResult::importedCommitCount,
            ImportResult::importedReferenceCount,
            ImportResult::importedGenericCount)
        .containsExactly(1L, numRefs, numGeneric);

    if (addJar) {
      soft.assertThat(
              targetPersist()
                  .fetchObjsIfExist(
                      new ObjId[] {repoObjId, commitObjId, contentObjId, referenceObjId}))
          .containsExactly(repoObj, commitObj, contentObj, referenceObj);
    } else {
      soft.assertThat(
              targetPersist()
                  .fetchObjsIfExist(
                      new ObjId[] {repoObjId, commitObjId, contentObjId, referenceObjId}))
          .containsOnlyNulls();
    }
  }
}

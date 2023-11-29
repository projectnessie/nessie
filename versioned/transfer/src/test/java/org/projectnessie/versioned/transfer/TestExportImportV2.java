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
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_REPOSITORY_ID;
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.io.IOException;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackendName;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.FileImporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;

@ExtendWith(PersistExtension.class)
@NessieBackendName(InmemoryBackendFactory.NAME)
public class TestExportImportV2 extends BaseExportImport {

  @NessiePersist
  @NessieStoreConfig(name = CONFIG_REPOSITORY_ID, value = "import-target")
  protected static Persist persistImport;

  @NessiePersist
  @NessieStoreConfig(name = CONFIG_REPOSITORY_ID, value = "export-source")
  protected static Persist persistExport;

  @Override
  ExportVersion exportVersion() {
    return ExportVersion.V2;
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
    // Don't initialize the repository, since the import with Persist already does that.
  }

  @Override
  ImportResult importRepo() throws IOException {
    NessieImporter importer =
        NessieImporter.builder()
            .persist(persistImport)
            .importFileSupplier(FileImporter.builder().sourceDirectory(dir).build())
            .build();
    return importer.importNessieRepository();
  }

  @Override
  ExportMeta exportRepo(boolean fullScan) throws IOException {
    NessieExporter exporter =
        NessieExporter.builder()
            .persist(persistExport)
            .fullScan(fullScan)
            .exportFileSupplier(FileExporter.builder().targetDirectory(dir).build())
            .build();
    return exporter.exportNessieRepository();
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
}

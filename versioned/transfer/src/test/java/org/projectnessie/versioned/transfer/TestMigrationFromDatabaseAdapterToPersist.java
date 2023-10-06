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
import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;
import org.projectnessie.versioned.storage.common.logic.RepositoryDescription;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.CloseableIterator;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackendName;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.FileImporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;

@ExtendWith({PersistExtension.class, DatabaseAdapterExtension.class})
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@NessieBackendName(InmemoryBackendFactory.NAME)
public class TestMigrationFromDatabaseAdapterToPersist extends BaseExportImport {
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;
  @NessieDbAdapter protected static VersionStore daVersionStore;

  @NessiePersist protected static Persist persist;

  @Override
  ExportVersion exportVersion() {
    return ExportVersion.V1;
  }

  @Override
  VersionStore sourceVersionStore() {
    return daVersionStore;
  }

  @Override
  VersionStore targetVersionStore() {
    return new VersionStoreImpl(persist);
  }

  @Override
  void prepareTargetRepo() {
    persist.erase();
    // Don't initialize the repository, since the import with Persist already does that.
  }

  @Override
  ImportResult importRepo() throws IOException {
    NessieImporter importer =
        NessieImporter.builder()
            .persist(persist)
            .importFileSupplier(FileImporter.builder().sourceDirectory(dir).build())
            .build();
    return importer.importNessieRepository();
  }

  @Override
  ExportMeta exportRepo(boolean fullScan) throws IOException {
    NessieExporter exporter =
        NessieExporter.builder()
            .databaseAdapter(databaseAdapter)
            .fullScan(fullScan)
            .exportFileSupplier(FileExporter.builder().targetDirectory(dir).build())
            .build();
    return exporter.exportNessieRepository();
  }

  @Override
  @MustBeClosed
  Stream<Hash> scanAllTargetCommits() {
    CloseableIterator<Obj> iter = persist.scanAllObjects(EnumSet.of(ObjType.COMMIT));
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false)
        .onClose(iter::close)
        .map(CommitObj.class::cast)
        .filter(o -> o.commitType() != CommitType.INTERNAL)
        .map(o -> Hash.of(o.id().asBytes()));
  }

  @Override
  protected void checkRepositoryDescription() {
    RepositoryDescription description =
        requireNonNull(repositoryLogic(persist).fetchRepositoryDescription());
    soft.assertThat(description.defaultBranchName()).isEqualTo("main");
    soft.assertThat(description.repositoryCreatedTime()).isNotNull();
    soft.assertThat(description.oldestPossibleCommitTime()).isNotNull();
    soft.assertThat(description.repositoryImportedTime()).isNotNull();
  }
}

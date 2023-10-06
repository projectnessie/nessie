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

import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;
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
public class TestMigrationFromPersistToDatabaseAdapter extends BaseExportImport {

  @NessieDbAdapter static DatabaseAdapter databaseAdapter;
  @NessieDbAdapter static VersionStore daVersionStore;

  @NessiePersist static Persist persist;

  @Override
  ExportVersion exportVersion() {
    return ExportVersion.V1;
  }

  @Override
  VersionStore sourceVersionStore() {
    return new VersionStoreImpl(persist);
  }

  @Override
  VersionStore targetVersionStore() {
    return daVersionStore;
  }

  @Override
  void prepareTargetRepo() {
    // Initialize repository w/o a default branch
    databaseAdapter.eraseRepo();
    databaseAdapter.initializeRepo("main");
    try {
      databaseAdapter.delete(BranchName.of("main"), Optional.empty());
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  ImportResult importRepo() throws IOException {
    NessieImporter importer =
        NessieImporter.builder()
            .databaseAdapter(databaseAdapter)
            .importFileSupplier(FileImporter.builder().sourceDirectory(dir).build())
            .build();
    return importer.importNessieRepository();
  }

  @Override
  ExportMeta exportRepo(boolean fullScan) throws IOException {
    NessieExporter exporter =
        NessieExporter.builder()
            .persist(persist)
            .exportVersion(1) // Must set export version to 1
            .fullScan(fullScan)
            .exportFileSupplier(FileExporter.builder().targetDirectory(dir).build())
            .build();
    return exporter.exportNessieRepository();
  }

  @Override
  @MustBeClosed
  Stream<Hash> scanAllTargetCommits() {
    return databaseAdapter.scanAllCommitLogEntries().map(CommitLogEntry::getHash);
  }

  @Override
  protected void checkRepositoryDescription() {}
}

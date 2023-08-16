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
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryTestConnectionProviderSource;
import org.projectnessie.versioned.persist.tests.extension.DatabaseAdapterExtension;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterName;
import org.projectnessie.versioned.persist.tests.extension.NessieExternalDatabase;
import org.projectnessie.versioned.transfer.files.FileExporter;
import org.projectnessie.versioned.transfer.files.FileImporter;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportMeta;
import org.projectnessie.versioned.transfer.serialize.TransferTypes.ExportVersion;

@ExtendWith(DatabaseAdapterExtension.class)
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
public class TestExportImportV1 extends BaseExportImport {

  @NessieDbAdapter
  @NessieDbAdapterConfigItem(name = "repository.id", value = "import-target")
  protected static DatabaseAdapter adapterImport;

  @NessieDbAdapter
  @NessieDbAdapterConfigItem(name = "repository.id", value = "import-target")
  protected static VersionStore storeImport;

  @NessieDbAdapter
  @NessieDbAdapterConfigItem(name = "repository.id", value = "export-source")
  protected static DatabaseAdapter adapterExport;

  @NessieDbAdapter
  @NessieDbAdapterConfigItem(name = "repository.id", value = "export-source")
  protected static VersionStore storeExport;

  @Override
  ExportVersion exportVersion() {
    return ExportVersion.V1;
  }

  @Override
  VersionStore sourceVersionStore() {
    return storeExport;
  }

  @Override
  VersionStore targetVersionStore() {
    return storeImport;
  }

  @Override
  void prepareTargetRepo() {
    // Initialize repository w/o a default branch
    adapterImport.eraseRepo();
    adapterImport.initializeRepo("main");
    try {
      adapterImport.delete(BranchName.of("main"), Optional.empty());
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  ImportResult importRepo() throws IOException {
    NessieImporter importer =
        NessieImporter.builder()
            .databaseAdapter(adapterImport)
            .importFileSupplier(FileImporter.builder().sourceDirectory(dir).build())
            .build();
    return importer.importNessieRepository();
  }

  @Override
  ExportMeta exportRepo(boolean fullScan) throws IOException {
    NessieExporter exporter =
        NessieExporter.builder()
            .databaseAdapter(adapterExport)
            .fullScan(fullScan)
            .exportFileSupplier(FileExporter.builder().targetDirectory(dir).build())
            .build();
    return exporter.exportNessieRepository();
  }

  @Override
  @MustBeClosed
  Stream<Hash> scanAllTargetCommits() {
    return adapterImport.scanAllCommitLogEntries().map(CommitLogEntry::getHash);
  }

  @Override
  protected void checkRepositoryDescription() {
    RepoDescription description = adapterImport.fetchRepositoryDescription();
    soft.assertThat(description.getProperties()).containsKey(RepoDescription.IMPORTED_AT_KEY);
  }
}

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

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.VersionStore;
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

@ExtendWith(SoftAssertionsExtension.class)
@ExtendWith({PersistExtension.class, DatabaseAdapterExtension.class})
@NessieDbAdapterName(InmemoryDatabaseAdapterFactory.NAME)
@NessieExternalDatabase(InmemoryTestConnectionProviderSource.class)
@NessieBackendName(InmemoryBackendFactory.NAME)
public class TestContentMigrationToPersist {
  @InjectSoftAssertions protected SoftAssertions soft;
  @NessieDbAdapter protected static DatabaseAdapter databaseAdapter;
  @NessieDbAdapter protected static VersionStore srcStore;

  @NessiePersist protected static Persist persist;

  @TempDir Path dir;

  private VersionStore prepareTargetRepo() {
    // Initialize repository w/o a default branch
    persist.erase();
    repositoryLogic(persist).initialize("main", false, b -> {});
    return new VersionStoreImpl(persist);
  }

  private void importRepo() throws IOException {
    NessieImporter importer =
        NessieImporter.builder()
            .persist(persist)
            .importFileSupplier(FileImporter.builder().sourceDirectory(dir).build())
            .build();
    importer.importNessieRepository();
  }

  private ExportMeta exportRepo(String branchName, int batchSize) throws IOException {
    NessieExporter exporter =
        NessieExporter.builder()
            .databaseAdapter(databaseAdapter)
            .versionStore(srcStore)
            .contentsFromBranch(branchName)
            .contentsBatchSize(batchSize)
            .exportFileSupplier(FileExporter.builder().targetDirectory(dir).build())
            .build();
    return exporter.exportNessieRepository();
  }

  @ParameterizedTest
  @CsvSource({
    "10, 1",
    "100, 3",
    "100, 10",
    "200, 1000",
  })
  void contentMigration(int numTables, int batchSize) throws Exception {
    ReferenceInfo<CommitMeta> main = srcStore.getNamedRef("main", GetNamedRefsParams.DEFAULT);
    BranchName branch = BranchName.of("test-branch");
    srcStore.create(branch, Optional.ofNullable(main.getHash()));
    Hash head = main.getHash();
    List<ContentKey> keys = new ArrayList<>();
    for (int i = 0; i < numTables; i++) {
      ContentKey key = ContentKey.of(branch.getName() + "-c-" + i);
      IcebergTable table =
          IcebergTable.of(
              "meta+" + branch.getName() + "-" + i + "-" + branch.getName(),
              42000 + i,
              43000 + i,
              44000 + i,
              45000 + i);
      keys.add(key);
      head =
          srcStore
              .commit(
                  branch,
                  Optional.of(head),
                  CommitMeta.fromMessage("commit #" + i + " " + branch),
                  Collections.singletonList(Put.of(key, table)))
              .getCommitHash();
    }

    Map<ContentKey, Content> values = srcStore.getValues(head, keys);

    ExportMeta exportMeta = exportRepo(branch.getName(), batchSize);
    soft.assertThat(exportMeta.getNamedReferencesCount()).isEqualTo(1);

    VersionStore targetStore = prepareTargetRepo();
    importRepo();

    ReferenceInfo<CommitMeta> targetBranch =
        targetStore.getNamedRef(branch.getName(), GetNamedRefsParams.DEFAULT);
    soft.assertThat(targetStore.getValues(targetBranch.getNamedRef(), keys)).isEqualTo(values);
  }
}

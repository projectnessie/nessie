/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.tiered.tests;

import java.util.ServiceLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.StringSerializer.TestEnum;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.tests.AbstractITVersionStore;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.tiered.adapter.SystemPropertiesConfigurer;
import org.projectnessie.versioned.tiered.impl.TieredVersionStore;

public class AbstractVersionStoreTest<CONFIG extends DatabaseAdapterConfig>
    extends AbstractITVersionStore {
  protected static DatabaseAdapter databaseAdapter;
  protected TieredVersionStore<String, String, String, TestEnum> versionStore;

  @BeforeEach
  void loadDatabaseAdapter() throws Exception {
    if (databaseAdapter == null) {
      @SuppressWarnings("unchecked")
      DatabaseAdapterFactory<CONFIG> factory =
          ServiceLoader.load(DatabaseAdapterFactory.class).iterator().next();

      databaseAdapter =
          factory
              .newBuilder()
              .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
              .configure(this::configureDatabaseAdapter)
              .build();
    }
    databaseAdapter.reinitializeRepo();

    StoreWorker<String, String, String, TestEnum> storeWorker =
        StoreWorker.of(
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            StringSerializer::mergeGlobalState,
            StringSerializer::extractGlobalState);

    versionStore = new TieredVersionStore<>(databaseAdapter, storeWorker);
  }

  protected CONFIG configureDatabaseAdapter(CONFIG config) {
    return config;
  }

  @AfterAll
  static void closeDatabaseAdapter() throws Exception {
    if (databaseAdapter != null) {
      databaseAdapter.close();
    }
  }

  @Override
  protected VersionStore<String, String, String, TestEnum> store() {
    return versionStore;
  }
}

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
import org.junit.jupiter.api.AfterEach;
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

public class TestVersionStore<CONFIG extends DatabaseAdapterConfig> extends AbstractITVersionStore {
  protected DatabaseAdapter databaseAdapter;
  protected TieredVersionStore<String, String, String, TestEnum> versionStore;

  @BeforeEach
  public void loadDatabaseAdapter() throws Exception {
    @SuppressWarnings("unchecked")
    DatabaseAdapterFactory<CONFIG> factory =
        ServiceLoader.load(DatabaseAdapterFactory.class).iterator().next();

    databaseAdapter =
        factory
            .newBuilder()
            .configure(SystemPropertiesConfigurer::configureFromSystemProperties)
            .configure(this::configureDatabaseAdapter)
            .build();
    databaseAdapter.initializeRepo();

    StoreWorker<String, String, String, TestEnum> storeWorker =
        StoreWorker.of(
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            StringSerializer.getInstanceNoSpy(),
            (value, state) -> value);

    versionStore = new TieredVersionStore<>(databaseAdapter, storeWorker);
  }

  protected CONFIG configureDatabaseAdapter(CONFIG config) {
    return config;
  }

  @AfterEach
  void closeDatabaseAdapter() throws Exception {
    databaseAdapter.close();
    postCloseDatabaseAdapter();
  }

  protected void postCloseDatabaseAdapter() {}

  @Override
  protected VersionStore<String, String, String, TestEnum> store() {
    return versionStore;
  }
}

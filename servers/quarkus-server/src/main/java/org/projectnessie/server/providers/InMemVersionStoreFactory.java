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
package org.projectnessie.server.providers;

import static org.projectnessie.server.config.VersionStoreConfig.VersionStoreType.INMEMORY;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import org.projectnessie.server.config.QuarkusDatabaseAdapterConfig;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.inmem.InmemoryDatabaseAdapterFactory;
import org.projectnessie.versioned.persist.inmem.InmemoryStore;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

/** In-memory version store factory. */
@StoreType(INMEMORY)
@Dependent
public class InMemVersionStoreFactory implements VersionStoreFactory {
  @Inject InmemoryStore store;
  @Inject QuarkusDatabaseAdapterConfig config;

  @Override
  public <VALUE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>>
      VersionStore<VALUE, METADATA, VALUE_TYPE> newStore(
          StoreWorker<VALUE, METADATA, VALUE_TYPE> worker, ServerConfig serverConfig) {
    DatabaseAdapter databaseAdapter =
        new InmemoryDatabaseAdapterFactory()
            .newBuilder()
            .withConfig(config)
            .withConnector(store)
            .build();

    databaseAdapter.initializeRepo(serverConfig.getDefaultBranch());

    return new PersistVersionStore<>(databaseAdapter, worker);
  }
}

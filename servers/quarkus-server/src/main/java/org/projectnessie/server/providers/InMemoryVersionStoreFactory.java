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
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapter;
import org.projectnessie.versioned.tiered.impl.TieredVersionStore;
import org.projectnessie.versioned.tiered.inmem.InmemoryDatabaseAdapterFactory;

/** In-memory version store factory. */
@StoreType(INMEMORY)
@Dependent
public class InMemoryVersionStoreFactory implements VersionStoreFactory {
  @Override
  public <VALUE, STATE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>>
      VersionStore<VALUE, STATE, METADATA, VALUE_TYPE> newStore(
          StoreWorker<VALUE, STATE, METADATA, VALUE_TYPE> worker, ServerConfig serverConfig) {
    DatabaseAdapter databaseAdapter =
        new InmemoryDatabaseAdapterFactory()
            .newBuilder()
            .configure(
                c -> {
                  DatabaseAdapter.DEFAULT_BRANCH.set(c, serverConfig.getDefaultBranch());
                })
            .build();

    // TODO update this piece !!
    try {
      databaseAdapter.initializeRepo();
    } catch (ReferenceConflictException e) {
      throw new RuntimeException(e);
    }

    return new TieredVersionStore<>(databaseAdapter, worker);
  }
}

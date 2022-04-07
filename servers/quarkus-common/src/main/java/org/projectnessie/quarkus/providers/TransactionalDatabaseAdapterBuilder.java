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
package org.projectnessie.quarkus.providers;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.TRANSACTIONAL;

import javax.enterprise.context.Dependent;
import javax.inject.Inject;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tx.TxConnectionConfig;
import org.projectnessie.versioned.persist.tx.TxConnectionProvider;
import org.projectnessie.versioned.persist.tx.TxDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tx.postgres.PostgresDatabaseAdapterFactory;

/** Transactional version store factory. */
@StoreType(TRANSACTIONAL)
@Dependent
public class TransactionalDatabaseAdapterBuilder implements DatabaseAdapterBuilder {
  @Inject TxDatabaseAdapterConfig config;
  @Inject TxConnectionProvider<TxConnectionConfig> connector;

  @Override
  public DatabaseAdapter newDatabaseAdapter(StoreWorker<?, ?, ?> storeWorker) {
    return new PostgresDatabaseAdapterFactory()
        .newBuilder()
        .withConfig(config)
        .withConnector(connector)
        .build(storeWorker);
  }
}

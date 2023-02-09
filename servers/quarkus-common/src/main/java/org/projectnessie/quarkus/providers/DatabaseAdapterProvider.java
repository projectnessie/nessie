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

import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.quarkus.providers.StoreType.Literal;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.TracingDatabaseAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class DatabaseAdapterProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseAdapterProvider.class);

  private final Instance<DatabaseAdapterBuilder> databaseAdapterBuilder;
  private final VersionStoreConfig storeConfig;
  private final ServerConfig serverConfig;

  @Inject
  public DatabaseAdapterProvider(
      @Any Instance<DatabaseAdapterBuilder> databaseAdapterBuilder,
      VersionStoreConfig storeConfig,
      ServerConfig serverConfig) {
    this.databaseAdapterBuilder = databaseAdapterBuilder;
    this.storeConfig = storeConfig;
    this.serverConfig = serverConfig;
  }

  @Produces
  @Singleton
  @Startup
  public DatabaseAdapter produceDatabaseAdapter() {
    VersionStoreType versionStoreType = storeConfig.getVersionStoreType();
    if (versionStoreType.isNewStorage()) {
      return null;
    }

    LOGGER.info("Using {} Version store", versionStoreType);

    DatabaseAdapter databaseAdapter =
        databaseAdapterBuilder.select(new Literal(versionStoreType)).get().newDatabaseAdapter();
    databaseAdapter.initializeRepo(serverConfig.getDefaultBranch());

    if (storeConfig.isTracingEnabled()) {
      databaseAdapter = new TracingDatabaseAdapter(databaseAdapter);
    }

    return databaseAdapter;
  }
}

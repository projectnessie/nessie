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

import io.quarkus.runtime.Startup;
import java.io.IOError;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.server.config.VersionStoreConfig;
import org.projectnessie.server.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.server.providers.StoreType.Literal;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.MetricsVersionStore;
import org.projectnessie.versioned.TracingVersionStore;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.spi.TracingDatabaseAdapter;
import org.projectnessie.versioned.persist.store.GenericContentVariantSupplier;
import org.projectnessie.versioned.persist.store.PersistVersionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A version store factory leveraging CDI to delegate to a {@code VersionStoreFactory} instance
 * based on the store type.
 */
@ApplicationScoped
public class ConfigurableVersionStoreFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConfigurableVersionStoreFactory.class);

  private final Instance<DatabaseAdapterBuilder> databaseAdapterBuilder;
  private final VersionStoreConfig storeConfig;
  private final ServerConfig serverConfig;

  /**
   * Configurable version store factory.
   *
   * @param databaseAdapterBuilder a CDI injector for {@code DatabaseAdapterBuilder}
   * @param storeConfig the version store configuration
   * @param serverConfig the server configuration
   */
  @Inject
  public ConfigurableVersionStoreFactory(
      @Any Instance<DatabaseAdapterBuilder> databaseAdapterBuilder,
      VersionStoreConfig storeConfig,
      ServerConfig serverConfig) {
    this.databaseAdapterBuilder = databaseAdapterBuilder;
    this.storeConfig = storeConfig;
    this.serverConfig = serverConfig;
  }

  /** Version store producer. */
  @Produces
  @Singleton
  @Startup
  public VersionStore<Content, CommitMeta, Content.Type> getVersionStore() {
    VersionStoreType versionStoreType = storeConfig.getVersionStoreType();

    try {
      LOGGER.info("Using {} Version store", versionStoreType);

      TableCommitMetaStoreWorker storeWorker = new TableCommitMetaStoreWorker();

      DatabaseAdapter databaseAdapter =
          databaseAdapterBuilder
              .select(new Literal(versionStoreType))
              .get()
              .newDatabaseAdapter(new GenericContentVariantSupplier<>(storeWorker));
      databaseAdapter.initializeRepo(serverConfig.getDefaultBranch());

      if (storeConfig.isTracingEnabled()) {
        databaseAdapter = new TracingDatabaseAdapter(databaseAdapter);
      }

      VersionStore<Content, CommitMeta, Content.Type> versionStore =
          new PersistVersionStore<>(databaseAdapter, storeWorker);

      if (storeConfig.isTracingEnabled()) {
        versionStore = new TracingVersionStore<>(versionStore);
      }
      if (storeConfig.isMetricsEnabled()) {
        versionStore = new MetricsVersionStore<>(versionStore);
      }

      return versionStore;
    } catch (RuntimeException | IOError e) {
      LOGGER.error("Failed to configure/start {} version store", versionStoreType, e);
      throw e;
    }
  }
}

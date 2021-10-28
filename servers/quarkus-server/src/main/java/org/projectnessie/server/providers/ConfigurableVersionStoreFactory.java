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
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.config.VersionStoreConfig;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.MetricsVersionStore;
import org.projectnessie.versioned.TracingVersionStore;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

/**
 * A version store factory leveraging CDI to delegate to a {@code VersionStoreFactory} instance
 * based on the store type.
 */
@ApplicationScoped
public class ConfigurableVersionStoreFactory {

  private final DatabaseAdapterFactory databaseAdapterFactory;
  private final VersionStoreConfig storeConfig;
  private final ServerConfig serverConfig;

  /**
   * Configurable version store factory.
   *
   * @param databaseAdapterFactory a CDI injector for {@code VersionStoreFactory}
   * @param storeConfig the version store configuration
   * @param serverConfig the server configuration
   */
  @Inject
  public ConfigurableVersionStoreFactory(
      DatabaseAdapterFactory databaseAdapterFactory,
      VersionStoreConfig storeConfig,
      ServerConfig serverConfig) {
    this.databaseAdapterFactory = databaseAdapterFactory;
    this.storeConfig = storeConfig;
    this.serverConfig = serverConfig;
  }

  /** Version store producer. */
  @Produces
  @Singleton
  @Startup
  public VersionStore<Contents, CommitMeta, Contents.Type> getVersionStore() {
    DatabaseAdapter databaseAdapter = databaseAdapterFactory.newDatabaseAdapter(serverConfig);
    databaseAdapter.initializeRepo(serverConfig.getDefaultBranch());

    VersionStore<Contents, CommitMeta, Contents.Type> versionStore =
        new PersistVersionStore<>(databaseAdapter, new TableCommitMetaStoreWorker());

    if (storeConfig.isTracingEnabled()) {
      versionStore = new TracingVersionStore<>(versionStore);
    }
    if (storeConfig.isMetricsEnabled()) {
      versionStore = new MetricsVersionStore<>(versionStore);
    }

    return versionStore;
  }
}

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
import java.io.IOError;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.MetricsVersionStore;
import org.projectnessie.versioned.TracingVersionStore;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
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

  private final VersionStoreConfig storeConfig;
  private final DatabaseAdapter databaseAdapter;

  /**
   * Configurable version store factory.
   *
   * @param storeConfig the version store configuration
   */
  @Inject
  public ConfigurableVersionStoreFactory(
      VersionStoreConfig storeConfig, DatabaseAdapter databaseAdapter) {
    this.storeConfig = storeConfig;
    this.databaseAdapter = databaseAdapter;
  }

  /** Version store producer. */
  @Produces
  @Singleton
  @Startup
  public VersionStore<Content, CommitMeta, Content.Type> getVersionStore() {
    VersionStoreType versionStoreType = storeConfig.getVersionStoreType();

    try {
      TableCommitMetaStoreWorker storeWorker = new TableCommitMetaStoreWorker();

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

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
package org.projectnessie.quarkus.providers.versionstore;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOError;
import java.util.function.Consumer;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.quarkus.providers.NotObserved;
import org.projectnessie.versioned.EventsVersionStore;
import org.projectnessie.versioned.Result;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.versionstore.VersionStoreImpl;
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
  private final Persist persist;
  private final Instance<Consumer<Result>> resultConsumer;

  /**
   * Configurable version store factory.
   *
   * @param storeConfig the version store configuration
   */
  @Inject
  public ConfigurableVersionStoreFactory(
      VersionStoreConfig storeConfig,
      @Default Persist persist,
      @Any Instance<Consumer<Result>> resultConsumer) {
    this.storeConfig = storeConfig;
    this.persist = persist;
    this.resultConsumer = resultConsumer;
  }

  /** Version store producer. */
  @Produces
  @Singleton
  @NotObserved
  public VersionStore getVersionStore() {
    VersionStoreType versionStoreType = storeConfig.getVersionStoreType();

    try {
      VersionStore versionStore = new VersionStoreImpl(persist);

      if (storeConfig.isEventsEnabled() && resultConsumer.isResolvable()) {
        versionStore = new EventsVersionStore(versionStore, resultConsumer.get());
      }
      return versionStore;
    } catch (RuntimeException | IOError e) {
      LOGGER.error("Failed to configure/start {} version store", versionStoreType, e);
      throw e;
    }
  }
}

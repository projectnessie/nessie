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
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.Instance;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.server.config.VersionStoreConfig;
import org.projectnessie.server.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.MetricsVersionStore;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TracingVersionStore;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.WithHash;
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

  private final Instance<VersionStoreFactory> versionStoreFactory;
  private final VersionStoreConfig storeConfig;
  private final ServerConfig serverConfig;

  /**
   * Configurable version store factory.
   *
   * @param versionStoreFactory a CDI injector for {@code VersionStoreFactory}
   * @param storeConfig the version store configuration
   * @param serverConfig the server configuration
   */
  @Inject
  public ConfigurableVersionStoreFactory(
      @Any Instance<VersionStoreFactory> versionStoreFactory,
      VersionStoreConfig storeConfig,
      ServerConfig serverConfig) {
    this.versionStoreFactory = versionStoreFactory;
    this.storeConfig = storeConfig;
    this.serverConfig = serverConfig;
  }

  private static final long START_RETRY_MIN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(2);
  private volatile long lastUnsuccessfulStart = 0L;

  /** Version store producer. */
  @Produces
  @Singleton
  @Startup
  public VersionStore<Contents, CommitMeta, Contents.Type> getVersionStore() {
    VersionStore<Contents, CommitMeta, Contents.Type> store = newVersionStore();
    try (Stream<WithHash<NamedRef>> str = store.getNamedRefs()) {
      if (!str.findFirst().isPresent()) {
        // if this is a new database, create a branch with the default branch name.
        try {
          store.create(BranchName.of(serverConfig.getDefaultBranch()), Optional.empty());
        } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException e) {
          LOGGER.warn("Failed to create default branch of {}.", serverConfig.getDefaultBranch(), e);
        }
      }
    }

    return store;
  }

  private VersionStore<Contents, CommitMeta, Contents.Type> newVersionStore() {
    final VersionStoreType versionStoreType = storeConfig.getVersionStoreType();
    if (System.nanoTime() - lastUnsuccessfulStart < START_RETRY_MIN_INTERVAL_NANOS) {
      LOGGER.warn("{} version store failed to start recently, try again later.", versionStoreType);
      throw new RuntimeException(
          String.format(
              "%s version store failed to start recently, try again later.", versionStoreType));
    }

    try {
      VersionStoreFactory factory =
          versionStoreFactory.select(new StoreType.Literal(versionStoreType)).get();
      LOGGER.info("Using {} Version store", versionStoreType);
      VersionStore<Contents, CommitMeta, Contents.Type> versionStore;
      try {
        versionStore = factory.newStore(new TableCommitMetaStoreWorker(), serverConfig);
      } catch (IOException e) {
        throw new IOError(e);
      }

      if (storeConfig.isTracingEnabled()) {
        versionStore = new TracingVersionStore<>(versionStore);
      }
      if (storeConfig.isMetricsEnabled()) {
        versionStore = new MetricsVersionStore<>(versionStore);
      }

      lastUnsuccessfulStart = 0L;
      return versionStore;
    } catch (RuntimeException | IOError e) {
      lastUnsuccessfulStart = System.nanoTime();
      LOGGER.error("Failed to configure/start {} version store", versionStoreType, e);
      throw e;
    }
  }
}

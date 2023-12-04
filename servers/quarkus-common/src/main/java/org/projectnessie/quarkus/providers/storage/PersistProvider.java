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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.versioned.storage.common.logic.Logics.repositoryLogic;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.projectnessie.quarkus.config.QuarkusStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.quarkus.providers.NotObserved;
import org.projectnessie.quarkus.providers.WIthInitializedRepository;
import org.projectnessie.quarkus.providers.versionstore.StoreType.Literal;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.storage.cache.CacheBackend;
import org.projectnessie.versioned.storage.cache.CacheSizing;
import org.projectnessie.versioned.storage.cache.PersistCaches;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class PersistProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PersistProvider.class);

  private final Instance<BackendBuilder> backendBuilder;
  private final Instance<Backend> backend;
  private final VersionStoreConfig versionStoreConfig;
  private final ServerConfig serverConfig;
  private final QuarkusStoreConfig storeConfig;

  @Inject
  public PersistProvider(
      @Any Instance<BackendBuilder> backendBuilder,
      @Any Instance<Backend> backend,
      VersionStoreConfig versionStoreConfig,
      QuarkusStoreConfig storeConfig,
      ServerConfig serverConfig) {
    this.backendBuilder = backendBuilder;
    this.backend = backend;
    this.versionStoreConfig = versionStoreConfig;
    this.storeConfig = storeConfig;
    this.serverConfig = serverConfig;
  }

  @Produces
  @Singleton
  public Backend produceBackend() {
    VersionStoreType versionStoreType = versionStoreConfig.getVersionStoreType();

    if (backendBuilder.isUnsatisfied()) {
      throw new IllegalStateException("No Quarkus backend implementation for " + versionStoreType);
    }

    return backendBuilder.select(new Literal(versionStoreType)).get().buildBackend();
  }

  public void closeBackend(@Disposes Backend backend) throws Exception {
    if (backend != null) {
      String info = backend.configInfo();
      if (!info.isEmpty()) {
        info = " (" + info + ")";
      }
      LOGGER.info("Stopping storage for {}{}", versionStoreConfig.getVersionStoreType(), info);
      backend.close();
    }
  }

  @Produces
  @Singleton
  @WIthInitializedRepository
  public Persist produceWithInitializedRepository(@Default Persist persist) {
    repositoryLogic(persist).initialize(serverConfig.getDefaultBranch());
    return persist;
  }

  @Produces
  @Singleton
  @NotObserved
  public Persist producePersist(MeterRegistry meterRegistry) {
    VersionStoreType versionStoreType = versionStoreConfig.getVersionStoreType();

    if (backend.isUnsatisfied()) {
      throw new IllegalStateException("No Quarkus backend for " + versionStoreType);
    }

    Backend b = backend.get();
    b.setupSchema();

    LOGGER.info("Creating/opening version store {} ...", versionStoreType);

    PersistFactory persistFactory = b.createFactory();
    Persist persist = persistFactory.newPersist(storeConfig);

    String info = b.configInfo();
    if (!info.isEmpty()) {
      info = " (" + info + ")";
    }

    CacheSizing cacheSizing =
        CacheSizing.builder()
            .fixedSizeInMB(storeConfig.cacheCapacityMB())
            .fractionMinSizeMb(storeConfig.cacheCapacityFractionMinSizeMb())
            .fractionOfMaxHeapSize(storeConfig.cacheCapacityFractionOfHeap())
            .heapSizeAdjustmentMB(storeConfig.cacheCapacityFractionAdjustMB())
            .build();
    int effectiveCacheSizeMB = cacheSizing.calculateEffectiveSizeInMB();

    String cacheInfo;
    if (effectiveCacheSizeMB > 0) {
      CacheBackend cacheBackend = PersistCaches.newBackend(effectiveCacheSizeMB, meterRegistry);
      persist = cacheBackend.wrap(persist);
      cacheInfo = "with " + effectiveCacheSizeMB + " MB objects cache";
    } else {
      cacheInfo = "without objects cache";
    }

    LOGGER.info("Using {} version store{}, {}", versionStoreType, info, cacheInfo);

    return persist;
  }
}

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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.Configurations.backendConfigBuilderApply;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;
import static org.projectnessie.tools.compatibility.internal.Util.throwUnchecked;
import static org.projectnessie.tools.compatibility.jersey.ServerConfigExtension.SERVER_CONFIG;

import java.net.URI;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.jersey.JerseyServer;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.logic.Logics;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.BackendFactory;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.PersistLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class CurrentNessieServer implements NessieServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(CurrentNessieServer.class);

  private final ServerKey serverKey;
  private final JerseyServer jersey;
  private final BooleanSupplier initializeRepository;

  private Persist persist;
  private AutoCloseable connectionProvider;

  static CurrentNessieServer currentNessieServer(
      ExtensionContext extensionContext,
      ServerKey serverKey,
      BooleanSupplier initializeRepository,
      Consumer<Object> backendConfigConsumer) {
    return extensionStore(extensionContext)
        .getOrComputeIfAbsent(
            serverKey,
            k -> new CurrentNessieServer(serverKey, initializeRepository, backendConfigConsumer),
            CurrentNessieServer.class);
  }

  @Override
  public URI getUri(Class<? extends NessieApi> apiType) {
    return Util.resolveNessieUri(jersey.getUri(), serverKey.getVersion(), apiType);
  }

  @Override
  public void close() throws Exception {
    try {
      if (jersey != null) {
        jersey.close();
      }
    } finally {
      if (connectionProvider != null) {
        LOGGER.info("Closing connection provider for current Nessie version");
        connectionProvider.close();
      }
    }
  }

  private CurrentNessieServer(
      ServerKey serverKey,
      BooleanSupplier initializeRepository,
      Consumer<Object> backendConfigConsumer) {
    try {
      this.serverKey = serverKey;
      this.initializeRepository = initializeRepository;
      this.jersey = new JerseyServer(() -> getOrCreatePersist(backendConfigConsumer));

      LOGGER.info("Nessie Server started at {}", jersey.getUri());
    } catch (Exception e) {
      throw throwUnchecked(e);
    }
  }

  private synchronized Persist getOrCreatePersist(Consumer<Object> backendConfigConsumer) {
    if (persist == null) {
      LOGGER.info("Creating Persist current Nessie version");
      Backend backend =
          createBackend(serverKey.getStorageName(), serverKey.getConfig(), backendConfigConsumer);
      connectionProvider = backend;

      persist = createPersist(backend, initializeRepository.getAsBoolean(), serverKey.getConfig());
    }
    return persist;
  }

  private static Persist createPersist(
      Backend backend, boolean initializeRepository, Map<String, String> configuration) {
    PersistFactory persistFactory = backend.createFactory();
    StoreConfig.Adjustable storeConfig = StoreConfig.Adjustable.empty();
    storeConfig = storeConfig.fromFunction(p -> configuration.get("nessie.store." + p));
    Persist persist = persistFactory.newPersist(storeConfig);

    if (initializeRepository) {
      persist.erase();
      Logics.repositoryLogic(persist).initialize(SERVER_CONFIG.getDefaultBranch());
    }

    return persist;
  }

  /**
   * Need to build the backend configuration and the backend here, because the originally intended
   * way to use {@code BackendFactory.newConfigInstance()} does not work: for example {@code
   * RocksDBBackendConfig} requires the {@code databasePath} attribute, but there is no way to
   * create a database-config instance using supplied configuration properties.
   */
  @SuppressWarnings("unchecked")
  private static <CONFIG> Backend createBackend(
      String backendName, Map<String, String> config, Consumer<Object> backendConfigConsumer) {
    BackendFactory<CONFIG> factory = PersistLoader.findFactoryByName(backendName);
    CONFIG backendConfig;
    try {
      Object backendConfigBuilder =
          Class.forName(factory.getClass().getName().replace("BackendFactory", "BackendConfig"))
              .getDeclaredMethod("builder")
              .invoke(null);
      backendConfigBuilderApply(backendConfigBuilder.getClass(), backendConfigBuilder, config::get);
      backendConfigConsumer.accept(backendConfigBuilder);
      backendConfig =
          (CONFIG) backendConfigBuilder.getClass().getMethod("build").invoke(backendConfigBuilder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    Backend backend = factory.buildBackend(backendConfig);
    backend.setupSchema();
    return backend;
  }
}

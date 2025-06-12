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

import static java.util.Arrays.asList;
import static org.projectnessie.tools.compatibility.internal.OldNessie.oldNessieClassLoader;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;
import static org.projectnessie.tools.compatibility.internal.Util.withClassLoader;
import static org.projectnessie.tools.compatibility.jersey.ServerConfigExtension.SERVER_CONFIG;

import java.net.URI;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.tools.compatibility.api.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OldNessieServer implements NessieServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(OldNessieServer.class);

  private final ServerKey serverKey;
  private final OldServerConnectionProvider connectionProvider;
  private final BooleanSupplier initializeRepository;
  private final ClassLoader classLoader;

  private Exception initError;
  private URI uri;
  private AutoCloseable jerseyServer;

  static OldNessieServer oldNessieServer(
      ExtensionContext context,
      ServerKey serverKey,
      BooleanSupplier initializeRepository,
      Consumer<Object> backendConfigConsumer) {

    Store store = extensionStore(context);

    ClassLoader classLoader = classLoader(store, serverKey.getVersion());

    OldServerConnectionProvider connectionProvider =
        oldConnectionProvider(store, classLoader, serverKey, backendConfigConsumer);

    return store.getOrComputeIfAbsent(
        serverKey,
        v -> new OldNessieServer(serverKey, connectionProvider, classLoader, initializeRepository),
        OldNessieServer.class);
  }

  private static OldServerConnectionProvider oldConnectionProvider(
      Store store,
      ClassLoader classLoader,
      ServerKey serverKey,
      Consumer<Object> backendConfigConsumer) {
    return store.getOrComputeIfAbsent(
        connectionProviderKey(serverKey),
        v -> new OldServerConnectionProvider(serverKey, classLoader, backendConfigConsumer),
        OldServerConnectionProvider.class);
  }

  private static String connectionProviderKey(ServerKey serverKey) {
    return "old-nessie-connection-provider-" + serverKey;
  }

  private static String classLoaderKey(Version version) {
    return "old-nessie-class-loader-" + version;
  }

  private static ClassLoader classLoader(Store store, Version version) {
    return store.getOrComputeIfAbsent(
        classLoaderKey(version),
        x -> {
          ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
          ClassLoader classLoader = createClassLoader(version);
          return new JerseyForOldServerClassLoader(version, classLoader, appClassLoader);
        },
        ClassLoader.class);
  }

  static ClassLoader createClassLoader(Version version) {
    List<String> artifactIds =
        asList(
            "nessie-compatibility-common",
            "nessie-versioned-storage-mongodb",
            "nessie-versioned-storage-rocksdb");
    try {
      return oldNessieClassLoader(version, artifactIds);
    } catch (DependencyResolutionException e) {
      throw new RuntimeException(
          "Failed to resolve dependencies for Nessie server version " + version, e);
    }
  }

  private OldNessieServer(
      ServerKey serverKey,
      OldServerConnectionProvider connectionProvider,
      ClassLoader classLoader,
      BooleanSupplier initializeRepository) {
    this.serverKey = serverKey;
    this.classLoader = classLoader;
    this.connectionProvider = connectionProvider;
    this.initializeRepository = initializeRepository;
  }

  @Override
  public URI getUri(Class<? extends NessieApi> apiType) {
    if (uri == null) {
      tryStart();
    }

    return Util.resolveNessieUri(uri, serverKey.getVersion(), apiType);
  }

  private void tryStart() {
    if (initError != null) {
      throw new IllegalStateException(
          String.format("Nessie Server for version %s not initialized", serverKey.getVersion()),
          initError);
    }

    try {
      withClassLoader(
          classLoader,
          () -> {
            Object persist = createPersist(classLoader);
            Supplier<?> persistsSupplier = () -> persist;

            Class<?> jerseyServerClass =
                classLoader.loadClass("org.projectnessie.tools.compatibility.jersey.JerseyServer");
            try {
              // Old signature, takes a Supplier<DatabaseAdapter> + Supplier<Persist> - MUST try
              // this one first!
              jerseyServer =
                  (AutoCloseable)
                      jerseyServerClass
                          .getConstructor(Supplier.class, Supplier.class)
                          .newInstance(null, persistsSupplier);
            } catch (NoSuchMethodException e) {
              jerseyServer =
                  (AutoCloseable)
                      jerseyServerClass
                          .getConstructor(Supplier.class)
                          .newInstance(persistsSupplier);
            }
            uri = (URI) jerseyServerClass.getMethod("getUri").invoke(jerseyServer);

            return null;
          });

      LOGGER.info(
          "Nessie Server for version {} with {} started at {} using {}",
          serverKey.getVersion(),
          serverKey.getStorageName(),
          uri,
          serverKey.getConfig());
    } catch (Exception e) {
      initError = e;
      throw new RuntimeException(
          String.format(
              "Failed to setup/start Nessie server for version %s with %s using %s",
              serverKey.getVersion(), serverKey.getStorageName(), serverKey.getConfig()),
          e);
    }
  }

  private Object createPersist(ClassLoader classLoader) throws Exception {
    Object backend = connectionProvider.connectionProvider;

    Class<?> classBackend =
        classLoader.loadClass("org.projectnessie.versioned.storage.common.persist.Backend");
    Class<?> classPersistFactory =
        classLoader.loadClass("org.projectnessie.versioned.storage.common.persist.PersistFactory");
    Class<?> classPersist =
        classLoader.loadClass("org.projectnessie.versioned.storage.common.persist.Persist");
    Class<?> classLogics =
        classLoader.loadClass("org.projectnessie.versioned.storage.common.logic.Logics");
    Class<?> classStoreConfig =
        classLoader.loadClass("org.projectnessie.versioned.storage.common.config.StoreConfig");
    Class<?> classStoreConfigAdjustable =
        classLoader.loadClass(
            "org.projectnessie.versioned.storage.common.config.StoreConfig$Adjustable");
    Class<?> classRepositoryLogic =
        classLoader.loadClass("org.projectnessie.versioned.storage.common.logic.RepositoryLogic");

    Object persistFactory = classBackend.getMethod("createFactory").invoke(backend);

    Object storeConfig = classStoreConfigAdjustable.getMethod("empty").invoke(null);
    storeConfig =
        classStoreConfigAdjustable
            .getMethod("fromFunction", Function.class)
            .invoke(
                storeConfig,
                (Function<String, String>) p -> serverKey.getConfig().get("nessie.store." + p));
    Object persist =
        classPersistFactory
            .getMethod("newPersist", classStoreConfig)
            .invoke(persistFactory, storeConfig);

    if (initializeRepository.getAsBoolean()) {
      classPersist.getMethod("erase").invoke(persist);
      Object repositoryLogic =
          classLogics.getMethod("repositoryLogic", classPersist).invoke(null, persist);
      classRepositoryLogic
          .getMethod("initialize", String.class)
          .invoke(repositoryLogic, SERVER_CONFIG.getDefaultBranch());
    }

    return persist;
  }

  @Override
  public void close() throws Exception {
    if (jerseyServer != null) {
      jerseyServer.close();
    }
  }
}

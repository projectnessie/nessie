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

import static org.projectnessie.tools.compatibility.internal.DependencyResolver.resolveToClassLoader;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;
import static org.projectnessie.tools.compatibility.internal.Util.withClassLoader;

import java.net.URI;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.collection.DependencyCollectionException;
import org.eclipse.aether.resolution.DependencyResolutionException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.projectnessie.tools.compatibility.api.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class OldNessieServer implements NessieServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(OldNessieServer.class);
  public static final String FALLBACK_ROCKSDB_VERSION = "6.28.2";

  private final ServerKey serverKey;
  private final OldServerConnectionProvider connectionProvider;
  private final BooleanSupplier initializeRepository;
  private final ClassLoader classLoader;

  private Exception initError;
  private URI uri;
  private AutoCloseable jerseyServer;

  static OldNessieServer oldNessieServer(
      ExtensionContext context, ServerKey serverKey, BooleanSupplier initializeRepository) {

    Store store = extensionStore(context);
    Store rootStore = extensionStore(context.getRoot());

    ClassLoader classLoader = classLoader(store, rootStore, serverKey.getVersion());

    OldServerConnectionProvider connectionProvider =
        oldConnectionProvider(store, classLoader, serverKey);

    return store.getOrComputeIfAbsent(
        serverKey,
        v -> new OldNessieServer(serverKey, connectionProvider, classLoader, initializeRepository),
        OldNessieServer.class);
  }

  private static OldServerConnectionProvider oldConnectionProvider(
      Store store, ClassLoader classLoader, ServerKey serverKey) {
    return store.getOrComputeIfAbsent(
        connectionProviderKey(serverKey),
        v -> new OldServerConnectionProvider(serverKey, classLoader),
        OldServerConnectionProvider.class);
  }

  private static String connectionProviderKey(ServerKey serverKey) {
    return "old-nessie-connection-provider-" + serverKey;
  }

  private static String classLoaderKey(Version version) {
    return "old-nessie-class-loader-" + version;
  }

  private static String oldSharedClassLoaderKey() {
    return "old-shared-nessie-class-loader";
  }

  private static ClassLoader classLoader(Store store, Store rootStore, Version version) {
    return store.getOrComputeIfAbsent(
        classLoaderKey(version),
        x -> {
          ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
          ClassLoader sharedClassLoader = sharedClassLoader(rootStore);
          ClassLoader classLoader = createClassLoader(version, sharedClassLoader);
          return new JerseyForOldServerClassLoader(version, classLoader, appClassLoader);
        },
        ClassLoader.class);
  }

  /**
   * Gets or creates the shared, global class loader for classes and resource that must not change.
   *
   * <p>This addresses the issue that RocksDB JNI must only exist once, at least on macOS. The issue
   * is that on macOS, rocksdbjni cannot be reloaded in different class loaders. Reloading
   * rocksdbjni however works fine on Linux.
   */
  private static ClassLoader sharedClassLoader(Store store) {
    return store.getOrComputeIfAbsent(
        oldSharedClassLoaderKey(), x -> createSharedClassLoader(), ClassLoader.class);
  }

  static ClassLoader createSharedClassLoader() {
    String rocksdbVersion = System.getProperty("rocksdb.version");
    if (rocksdbVersion == null) {
      rocksdbVersion = FALLBACK_ROCKSDB_VERSION;
      LOGGER.warn(
          "System property rocksdb.version not present, using {} as the default for the org.rocksdb:rocksdbjni artifact",
          rocksdbVersion);
    }
    Artifact rocksDbArtifact =
        new DefaultArtifact("org.rocksdb", "rocksdbjni", "jar", rocksdbVersion);
    try {
      return resolveToClassLoader(rocksDbArtifact.toString(), rocksDbArtifact, null);
    } catch (DependencyCollectionException | DependencyResolutionException e) {
      throw new RuntimeException("Failed to resolve dependencies for RocksDB", e);
    }
  }

  static ClassLoader createClassLoader(Version version, ClassLoader sharedClassLoader) {
    // Use 'nessie-jaxrs' because it has all the necessary dependencies to the DatabaseAdapter
    // implementations, REST services, Version store implementation, etc.
    Artifact nessieClientArtifact =
        new DefaultArtifact("org.projectnessie", "nessie-jaxrs", "jar", version.toString());
    try {
      return resolveToClassLoader(version.toString(), nessieClientArtifact, sharedClassLoader);
    } catch (DependencyCollectionException | DependencyResolutionException e) {
      throw new RuntimeException(
          "Failed to resolve dependencies for Nessie client version " + version, e);
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
  public URI getUri() {
    if (uri == null) {
      tryStart();
    }
    return uri;
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
            Class<?> databaseConnectionProviderClass =
                classLoader.loadClass(
                    "org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider");
            Object databaseAdapter =
                classLoader
                    .loadClass("org.projectnessie.tools.compatibility.jersey.DatabaseAdapters")
                    .getMethod(
                        "createDatabaseAdapter", String.class, databaseConnectionProviderClass)
                    .invoke(
                        null,
                        serverKey.getDatabaseAdapterName(),
                        connectionProvider.connectionProvider);

            // Call eraseRepo + initializeRepo only when requested. This is always true for
            // old-clients and old-servers test cases (once per test class + Nessie version).
            // Upgrade tests initialize the repository only once.
            if (initializeRepository.getAsBoolean()) {
              LOGGER.info(
                  "Initializing database adapter repository for Nessie Server version {} with {} using {}",
                  serverKey.getVersion(),
                  serverKey.getDatabaseAdapterName(),
                  serverKey.getDatabaseAdapterConfig());
              try {
                databaseAdapter.getClass().getMethod("eraseRepo").invoke(databaseAdapter);
              } catch (NoSuchMethodException e) {
                // ignore, older Nessie versions do not have the 'eraseRepo' method
              }
              databaseAdapter
                  .getClass()
                  .getMethod("initializeRepo", String.class)
                  .invoke(databaseAdapter, "main");
            }

            Supplier<?> databaseAdapterSupplier = () -> databaseAdapter;

            Class<?> jerseyServerClass =
                classLoader.loadClass("org.projectnessie.tools.compatibility.jersey.JerseyServer");
            jerseyServer =
                (AutoCloseable)
                    jerseyServerClass
                        .getConstructor(Supplier.class)
                        .newInstance(databaseAdapterSupplier);
            uri = (URI) jerseyServerClass.getMethod("getUri").invoke(jerseyServer);

            return null;
          });

      LOGGER.info(
          "Nessie Server for version {} with {} started at {} using {}",
          serverKey.getVersion(),
          serverKey.getDatabaseAdapterName(),
          uri,
          serverKey.getDatabaseAdapterConfig());
    } catch (Exception e) {
      initError = e;
      throw new RuntimeException(
          String.format(
              "Failed to setup/start Nessie server for version %s with %s using %s",
              serverKey.getVersion(),
              serverKey.getDatabaseAdapterName(),
              serverKey.getDatabaseAdapterConfig()),
          e);
    }
  }

  @Override
  public void close() throws Throwable {
    if (jerseyServer != null) {
      jerseyServer.close();
    }
  }
}

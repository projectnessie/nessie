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
import static java.util.Collections.singletonList;
import static org.projectnessie.tools.compatibility.api.Version.COMPAT_COMMON_DEPENDENCIES_START;
import static org.projectnessie.tools.compatibility.api.Version.VERSIONED_REST_URI_START;
import static org.projectnessie.tools.compatibility.internal.OldNessie.oldNessieClassLoader;
import static org.projectnessie.tools.compatibility.internal.Util.extensionStore;
import static org.projectnessie.tools.compatibility.internal.Util.withClassLoader;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
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
      ExtensionContext context, ServerKey serverKey, BooleanSupplier initializeRepository) {

    Store store = extensionStore(context);

    ClassLoader classLoader = classLoader(store, serverKey.getVersion());

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
    // The 'nessie-jaxrs' has all the necessary dependencies to the DatabaseAdapter
    // implementations, REST services, Version store implementation, etc. in older versions.
    // Newer versions declare what is required as runtime dependencies of
    // `nessie-compatibility-common`, except mongodb
    List<String> artifactIds =
        version.isGreaterThanOrEqual(COMPAT_COMMON_DEPENDENCIES_START)
            ? asList(
                "nessie-compatibility-common",
                "nessie-versioned-persist-mongodb",
                "nessie-versioned-persist-mongodb-test")
            : singletonList("nessie-jaxrs");
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

    if (serverKey.getVersion().isLessThan(VERSIONED_REST_URI_START)) {
      return uri;
    }

    return Util.resolveNessieUri(uri, apiType);
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
            Class<?> adaptersFactoryClass =
                classLoader.loadClass(
                    "org.projectnessie.tools.compatibility.jersey.DatabaseAdapters");

            List<Object> createParams = new ArrayList<>();
            createParams.add(serverKey.getDatabaseAdapterName());
            createParams.add(connectionProvider.connectionProvider);
            Method createAdapterMethod;
            try {
              createAdapterMethod =
                  adaptersFactoryClass.getMethod(
                      "createDatabaseAdapter",
                      String.class,
                      databaseConnectionProviderClass,
                      Map.class);

              createParams.add(serverKey.getDatabaseAdapterConfig());
            } catch (NoSuchMethodException e) {
              // Fallback for the method without the config Map (support older servers)
              createAdapterMethod =
                  adaptersFactoryClass.getMethod(
                      "createDatabaseAdapter", String.class, databaseConnectionProviderClass);
            }

            Object databaseAdapter = createAdapterMethod.invoke(null, createParams.toArray());

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

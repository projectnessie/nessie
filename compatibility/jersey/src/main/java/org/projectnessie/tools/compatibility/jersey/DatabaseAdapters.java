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
package org.projectnessie.tools.compatibility.jersey;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Map;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;
import org.projectnessie.versioned.persist.tests.SystemPropertiesConfigurer;
import org.projectnessie.versioned.persist.tests.extension.TestConnectionProviderSource;

/**
 * Helper class to configure and create database adapters within the class path of old Nessie
 * versions.
 *
 * <p>This class and its companion classes in this package are used from {@code
 * org.projectnessie.tools.compatibility.internal.OldNessieServer} via {@code
 * org.projectnessie.tools.compatibility.internal.JerseyForOldServerClassLoader} and have access to
 * an old Nessie server's class path.
 */
public final class DatabaseAdapters {
  private DatabaseAdapters() {}

  public static DatabaseConnectionProvider<DatabaseConnectionConfig>
      createDatabaseConnectionProvider(
          String databaseAdapterName, Map<String, String> configuration) {
    DatabaseAdapterFactory<
            DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
        factory = DatabaseAdapterFactory.loadFactoryByName(databaseAdapterName);

    String providerSpec =
        databaseAdapterName.indexOf(':') == -1
            ? null
            : databaseAdapterName
                .substring(databaseAdapterName.indexOf(':') + 1)
                .toLowerCase(Locale.ROOT);

    DatabaseAdapterFactory.Builder<
            DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
        builder = factory.newBuilder();

    TestConnectionProviderSource<DatabaseConnectionConfig> providerSource =
        TestConnectionProviderSource.findCompatibleProviderSource(
            builder.getConfig(), factory, providerSpec);
    providerSource.configureConnectionProviderConfigFromDefaults(
        config ->
            SystemPropertiesConfigurer.configureFromPropertiesGeneric(
                config,
                DatabaseConnectionConfig.class,
                prop -> configuration.getOrDefault(prop, System.getProperty(prop))));
    try {
      // createConnectionProvider method is protected, not defined on DatabaseConnectionProvider,
      // but on AbstractTestConnectionProviderSource
      Method createConnectionProviderMethod =
          providerSource.getClass().getMethod("createConnectionProvider");
      @SuppressWarnings("unchecked")
      DatabaseConnectionProvider<DatabaseConnectionConfig> connectionProvider =
          (DatabaseConnectionProvider<DatabaseConnectionConfig>)
              createConnectionProviderMethod.invoke(providerSource);

      connectionProvider.configure(providerSource.getConnectionProviderConfig());
      connectionProvider.initialize();

      return connectionProvider;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static DatabaseAdapter createDatabaseAdapter(
      String databaseAdapterName,
      DatabaseConnectionProvider<DatabaseConnectionConfig> connectionProvider) {
    DatabaseAdapterFactory<
            DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
        factory = DatabaseAdapterFactory.loadFactoryByName(databaseAdapterName);

    DatabaseAdapterFactory.Builder<
            DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
        builder = factory.newBuilder();

    builder.withConnector(connectionProvider);
    try {
      // Signature 'public DatabaseAdapter build(StoreWorker storeWorker)'
      return buildWithStoreWorker(builder);
    } catch (NoSuchMethodException | ClassNotFoundException | NoClassDefFoundError e) {
      try {
        // Signature 'public DatabaseAdapter build(ContentVariantSupplier contentVariantSupplier)'
        return buildWithContentVariantSupplier(builder);
      } catch (NoSuchMethodException
          | ClassNotFoundException
          | NoClassDefFoundError
          | InvocationTargetException
          | InstantiationException
          | IllegalAccessException e2) {
        try {
          // "Old" signature 'public DatabaseAdapter build()'
          //noinspection JavaReflectionMemberAccess
          return buildPre019(builder, DatabaseAdapterFactory.Builder.class.getMethod("build"));
        } catch (NoSuchMethodException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  private static DatabaseAdapter buildPre019(
      DatabaseAdapterFactory.Builder<
              DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
          builder,
      Method build) {
    return doBuild(builder, build);
  }

  private static DatabaseAdapter buildWithStoreWorker(
      DatabaseAdapterFactory.Builder<
              DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
          builder)
      throws NoSuchMethodException, ClassNotFoundException {
    Method build = DatabaseAdapterFactory.Builder.class.getMethod("build", StoreWorker.class);
    return doBuild(builder, build, new TableCommitMetaStoreWorker());
  }

  private static DatabaseAdapter buildWithContentVariantSupplier(
      DatabaseAdapterFactory.Builder<
              DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
          builder)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException,
          InstantiationException, IllegalAccessException {
    Method build =
        DatabaseAdapterFactory.Builder.class.getMethod(
            "build",
            Class.forName("org.projectnessie.versioned.persist.adapter.ContentVariantSupplier"));
    Object cvs =
        Class.forName("org.projectnessie.versioned.persist.store.GenericContentVariantSupplier")
            .getDeclaredConstructor(StoreWorker.class)
            .newInstance(new TableCommitMetaStoreWorker());
    return doBuild(builder, build, cvs);
  }

  private static DatabaseAdapter doBuild(
      DatabaseAdapterFactory.Builder<
              DatabaseAdapterConfig, DatabaseAdapterConfig, DatabaseConnectionProvider<?>>
          builder,
      Method build,
      Object... args) {
    try {
      return (DatabaseAdapter) build.invoke(builder, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}

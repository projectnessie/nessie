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
package org.projectnessie.versioned.persist.tests.extension;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;

/**
 * Manages all external resources (Docker containers, processes) and "internal" resources (network
 * connections and pools) required for tests.
 *
 * <p>There are two ways to choose a {@link TestConnectionProviderSource} implementation:
 *
 * <ol>
 *   <li>In JUnit tests, reference the implementation class directly using the {@link
 *       NessieExternalDatabase} annotation.
 *   <li>Via {@link #findCompatibleProviderSource(DatabaseAdapterConfig, DatabaseAdapterFactory,
 *       String)} using a code snippet like this:
 *       <pre><code>
 *   DatabaseAdapterConfig config = ...;
 *   String providerSpec =
 *       adapter.indexOf(':') == -1
 *           ? null
 *           : adapter.substring(adapter.indexOf(':') + 1)
 *               .toLowerCase(Locale.ROOT);
 *   TestConnectionProviderSource&lt;DatabaseConnectionConfig&gt; providerSource =
 *       findCompatibleProviderSource(
 *           config, factory, providerSpec);
 *   providerSource
 *       .configureConnectionProviderConfigFromDefaults(
 *           SystemPropertiesConfigurer
 *               ::configureConnectionFromSystemProperties);
 *   try {
 *     providerSource.start();
 *   } catch (Exception e) {
 *     throw new RuntimeException(e);
 *   }
 *   connectionProvider = providerSource.getConnectionProvider();
 * </code></pre>
 * </ol>
 */
public interface TestConnectionProviderSource<CONN_CONFIG extends DatabaseConnectionConfig> {

  boolean isCompatibleWith(
      DatabaseAdapterConfig adapterConfig, DatabaseAdapterFactory<?, ?, ?> databaseAdapterFactory);

  /** Creates the default {@link DatabaseConnectionConfig}. */
  CONN_CONFIG createDefaultConnectionProviderConfig();

  /**
   * A shortcut for {@link #setConnectionProviderConfig(DatabaseConnectionConfig)} using a
   * configuration received from {@link #createDefaultConnectionProviderConfig()} and passed through
   * the given function.
   */
  void configureConnectionProviderConfigFromDefaults(Function<CONN_CONFIG, CONN_CONFIG> configurer);

  /**
   * Set the configuration for the {@link DatabaseConnectionProvider} created when {@link #start()}
   * is invoked.
   */
  void setConnectionProviderConfig(CONN_CONFIG connectionProviderConfig);

  CONN_CONFIG getConnectionProviderConfig();

  /**
   * Returns the preconfigured connection provider.
   *
   * <p>This method should be called after {@link #start()}.
   */
  DatabaseConnectionProvider<CONN_CONFIG> getConnectionProvider();

  /**
   * Initialize/start the connection provider.
   *
   * <p>Implementations start for example Docker containers or external processes and setup
   * connection pools.
   */
  void start() throws Exception;

  /**
   * Stop the connection provider and release all held resources.
   *
   * <p>Implementations for example tear down connection pools and stop example Docker containers or
   * external processes.
   */
  void stop() throws Exception;

  /**
   * Tries to find a {@link TestConnectionProviderSource} implementation that returns {@code true}
   * for {@link #isCompatibleWith(DatabaseAdapterConfig, DatabaseAdapterFactory)} for the given
   * arguments using Java's {@link ServiceLoader} mechanism.
   *
   * @param databaseAdapterConfig database-adapter configuration, passed to {@link
   *     #isCompatibleWith(DatabaseAdapterConfig, DatabaseAdapterFactory)}
   * @param databaseAdapterFactory database-adapter-factory, passed to {@link
   *     #isCompatibleWith(DatabaseAdapterConfig, DatabaseAdapterFactory)}
   * @param providerSpec optional string, if non-{@code null} the lower-case class-name of the
   *     {@link TestConnectionProviderSource} implementation must contain this string
   * @return {@link TestConnectionProviderSource} implementation
   * @throws IllegalStateException if no matching {@link TestConnectionProviderSource}
   *     implementation could be found
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  static <CONN_CONFIG extends DatabaseConnectionConfig>
      TestConnectionProviderSource<CONN_CONFIG> findCompatibleProviderSource(
          DatabaseAdapterConfig databaseAdapterConfig,
          DatabaseAdapterFactory<?, ?, ?> databaseAdapterFactory,
          String providerSpec) {
    List<TestConnectionProviderSource> providerSources = new ArrayList<>();
    for (TestConnectionProviderSource ps : ServiceLoader.load(TestConnectionProviderSource.class)) {
      if (ps.isCompatibleWith(databaseAdapterConfig, databaseAdapterFactory)) {
        providerSources.add(ps);
      }
    }

    if (providerSources.isEmpty()) {
      throw new IllegalStateException(
          "No matching TestConnectionProviderSource found for " + databaseAdapterConfig);
    }

    if (providerSpec != null) {
      providerSources.removeIf(
          ps -> !ps.getClass().getName().toLowerCase(Locale.ROOT).contains(providerSpec));
    }

    if (providerSources.size() != 1) {
      throw new IllegalStateException(
          "Too many TestConnectionProviderSource instances matched: "
              + providerSources.stream().map(Object::toString).collect(Collectors.joining(", ")));
    }

    return providerSources.get(0);
  }
}

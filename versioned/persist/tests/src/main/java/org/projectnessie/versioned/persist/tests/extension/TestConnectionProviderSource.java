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

import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;

/**
 * Manages all external resources (Docker containers, processes) and "internal" resources (network
 * connections and pools) required for tests.
 */
public interface TestConnectionProviderSource<T extends DatabaseConnectionProvider<?>> {

  /**
   * Updates the given {@code config} with the {@link DatabaseConnectionProvider} via {@link
   * DatabaseAdapterConfig#withConnectionProvider(DatabaseConnectionProvider)}.
   *
   * @param config the current configuration
   * @return {@code config} with a valid {@link DatabaseConnectionProvider} instance.
   */
  DatabaseAdapterConfig<T> updateConfig(DatabaseAdapterConfig<T> config);

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
}

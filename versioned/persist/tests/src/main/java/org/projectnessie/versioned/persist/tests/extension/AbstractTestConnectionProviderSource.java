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

import java.util.function.Function;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionConfig;
import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;

public abstract class AbstractTestConnectionProviderSource<
        CONN_CONFIG extends DatabaseConnectionConfig>
    implements TestConnectionProviderSource<CONN_CONFIG> {
  private CONN_CONFIG config;
  private DatabaseConnectionProvider<CONN_CONFIG> connectionProvider;

  @Override
  public void configureConnectionProviderConfigFromDefaults(
      Function<CONN_CONFIG, CONN_CONFIG> configurer) {
    CONN_CONFIG config = createDefaultConnectionProviderConfig();
    config = configurer.apply(config);
    setConnectionProviderConfig(config);
  }

  @Override
  public void setConnectionProviderConfig(CONN_CONFIG connectionProviderConfig) {
    this.config = connectionProviderConfig;
  }

  @Override
  public DatabaseConnectionProvider<CONN_CONFIG> getConnectionProvider() {
    return connectionProvider;
  }

  @Override
  public CONN_CONFIG getConnectionProviderConfig() {
    return config;
  }

  /** Creates an empty {@link DatabaseConnectionProvider} of a suitable sub-type. */
  protected abstract DatabaseConnectionProvider<CONN_CONFIG> createConnectionProvider();

  @Override
  public void start() throws Exception {
    if (connectionProvider != null) {
      throw new IllegalStateException("Already started");
    }
    connectionProvider = createConnectionProvider();
    connectionProvider.configure(config);
    connectionProvider.initialize();
  }

  @Override
  public void stop() throws Exception {
    try {
      if (connectionProvider != null) {
        connectionProvider.close();
      }
    } finally {
      connectionProvider = null;
    }
  }
}

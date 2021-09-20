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
package org.projectnessie.versioned.persist.tx.h2;

import java.sql.SQLException;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.TestConnectionProviderSource;
import org.projectnessie.versioned.persist.tx.TxConnectionProvider;
import org.projectnessie.versioned.persist.tx.local.ImmutableLocalTxConnectionConfig;
import org.projectnessie.versioned.persist.tx.local.LocalConnectionProvider;

public class H2TestConnectionProviderSource
    implements TestConnectionProviderSource<TxConnectionProvider<?>> {
  private LocalConnectionProvider connectionProvider;

  @Override
  public void start() throws SQLException {
    connectionProvider = new LocalConnectionProvider();
    connectionProvider.configure(
        ImmutableLocalTxConnectionConfig.builder().jdbcUrl("jdbc:h2:mem:nessie").build());
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

  @Override
  public DatabaseAdapterConfig<TxConnectionProvider<?>> updateConfig(
      DatabaseAdapterConfig<TxConnectionProvider<?>> config) {
    return config.withConnectionProvider(connectionProvider);
  }
}

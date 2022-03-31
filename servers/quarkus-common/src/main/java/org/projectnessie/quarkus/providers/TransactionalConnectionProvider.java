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
package org.projectnessie.quarkus.providers;

import io.agroal.api.AgroalDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.projectnessie.quarkus.config.QuarkusVersionStoreAdvancedConfig;
import org.projectnessie.versioned.persist.tx.ImmutableDefaultTxConnectionConfig;
import org.projectnessie.versioned.persist.tx.TxConnectionConfig;
import org.projectnessie.versioned.persist.tx.TxConnectionProvider;

@Singleton
public class TransactionalConnectionProvider {
  @Inject AgroalDataSource dataSource;
  @Inject QuarkusVersionStoreAdvancedConfig storeConfig;

  @Produces
  @Singleton
  public TxConnectionProvider<TxConnectionConfig> produceConnectionProvider() throws SQLException {
    TxConnectionProvider<TxConnectionConfig> connectionProvider =
        new TxConnectionProvider<>() {
          @Override
          public void close() {}

          @Override
          public Connection borrowConnection() throws SQLException {
            Connection conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            return conn;
          }
        };

    ImmutableDefaultTxConnectionConfig.Builder connectionConfig =
        ImmutableDefaultTxConnectionConfig.builder();
    if (!storeConfig.getJdbcCatalog().isEmpty()) {
      connectionConfig.catalog(storeConfig.getJdbcCatalog());
    }
    if (!storeConfig.getJdbcSchema().isEmpty()) {
      connectionConfig.catalog(storeConfig.getJdbcSchema());
    }

    connectionProvider.configure(connectionConfig.build());
    connectionProvider.initialize();

    return connectionProvider;
  }
}

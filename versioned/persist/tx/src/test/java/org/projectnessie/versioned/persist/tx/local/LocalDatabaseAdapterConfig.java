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
package org.projectnessie.versioned.persist.tx.local;

import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.persist.tx.DefaultTxDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tx.TxConnectionProvider;
import org.projectnessie.versioned.persist.tx.TxDatabaseAdapterConfig;

/**
 * TX-database-adapter config interface.
 *
 * <p>See {@link DefaultTxDatabaseAdapterConfig} for the default, immutable-annotated type.
 */
public interface LocalDatabaseAdapterConfig extends TxDatabaseAdapterConfig {

  @Override
  @Value.Default
  default TxConnectionProvider getConnectionProvider() {
    return new LocalConnectionProvider();
  }

  /**
   * JDBC URL, only used, if {@link #getConnectionProvider} returns {@code null}, only for
   * non-production scenarios.
   */
  @Nullable
  String getJdbcUrl();

  LocalDatabaseAdapterConfig withJdbcUrl(String jdbcUrl);

  /**
   * Database user, only used, if {@link #getConnectionProvider} returns {@code null}, only for
   * non-production scenarios.
   */
  @Nullable
  String getJdbcUser();

  LocalDatabaseAdapterConfig withJdbcUser(String jdbcUser);

  /**
   * Database password, only used, if {@link #getConnectionProvider} returns {@code null}, only for
   * non-production scenarios.
   */
  @Nullable
  String getJdbcPass();

  LocalDatabaseAdapterConfig withJdbcPass(String jdbcPass);

  /** For "unmanaged" connection-pools: the JDBC pool's minimum size, defaults to {@code 1}. */
  @Value.Default
  default int getPoolMinSize() {
    return 1;
  }

  LocalDatabaseAdapterConfig withPoolMinSize(int poolMinSize);

  /** For "unmanaged" connection-pools: the JDBC pool's maximum size, defaults to {@code 10}. */
  @Value.Default
  default int getPoolMaxSize() {
    return 10;
  }

  LocalDatabaseAdapterConfig withPoolMaxSize(int poolMaxSize);

  /** For "unmanaged" connection-pools: the JDBC pool's initial size, defaults to {@code 1}. */
  @Value.Default
  default int getPoolInitialSize() {
    return 1;
  }

  LocalDatabaseAdapterConfig withPoolInitialSize(int poolInitialSize);

  /**
   * For "unmanaged" connection-pools: the JDBC pool's connection acquisition timeout in seconds,
   * defaults to {@code 30}.
   */
  @Value.Default
  default int getPoolAcquisitionTimeoutSeconds() {
    return 30;
  }

  LocalDatabaseAdapterConfig withPoolAcquisitionTimeoutSeconds(int poolAcquisitionTimeoutSeconds);

  /**
   * For "unmanaged" connection-pools: the JDBC pool's connection max lifetime in minutes, defaults
   * to {@code 5}.
   */
  @Value.Default
  default int getPoolConnectionLifetimeMinutes() {
    return 5;
  }

  LocalDatabaseAdapterConfig withPoolConnectionLifetimeMinutes(int poolConnectionLifetimeMinutes);

  /**
   * For "unmanaged" connection-pools: the JDBC pool's transaction isolation, one of the enums from
   * {@link io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation},
   * defaults to {@link
   * io.agroal.api.configuration.AgroalConnectionFactoryConfiguration.TransactionIsolation#READ_COMMITTED}.
   *
   * <p>Technically we only need {@code READ_COMMITTED}. The extra bit that {@code SERIALIZABLE}
   * brings is not needed. Reason is that we "only" do CAS-ish operations on 'global_state' and
   * 'named_refs' tables (think: {@code INSERT INTO resp. UPDATE value=? ... WHERE value=?}) - the
   * other operations (on 'commit_log' and 'key_list') are {@code INSERT INTO} only. All "guarded"
   * by a transaction - so either everything or nothing becomes visible atomically.
   */
  @Value.Default
  default String getPoolTransactionIsolation() {
    return TransactionIsolation.READ_COMMITTED.name();
  }

  LocalDatabaseAdapterConfig withPoolTransactionIsolation(String poolTransactionIsolation);
}

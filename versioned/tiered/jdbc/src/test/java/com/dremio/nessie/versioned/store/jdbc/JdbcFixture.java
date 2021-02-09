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
package com.dremio.nessie.versioned.store.jdbc;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

import com.dremio.nessie.versioned.impl.AbstractTieredStoreFixture;
import com.dremio.nessie.versioned.store.jdbc.ImmutableJdbcStoreConfig.Builder;

import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

public abstract class JdbcFixture extends AbstractTieredStoreFixture<JdbcStore, JdbcStoreConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcFixture.class);

  private static DataSource dataSource;
  private static JdbcDatabaseContainer<?> databaseContainer;

  public JdbcFixture() {
    super(makeConfig(true));
  }

  private static JdbcStoreConfig makeConfig(boolean setupTables) {
    Builder builder = JdbcStoreConfig.builder()
        .setupTables(setupTables);

    if (System.getProperty("it.nessie.store.jdbc.table_prefix") != null) {
      builder.tablePrefix(System.getProperty("it.nessie.store.jdbc.table_prefix"));
    }
    if (System.getProperty("it.nessie.store.jdbc.schema") != null) {
      builder.schema(System.getProperty("it.nessie.store.jdbc.schema"));
    }
    if (System.getProperty("it.nessie.store.jdbc.catalog") != null) {
      builder.catalog(System.getProperty("it.nessie.store.jdbc.catalog"));
    }
    return builder.build();
  }

  @Override
  public JdbcStore createStoreImpl() {
    try {
      JdbcStoreConfig cfg = makeConfig(dataSource == null);
      DatabaseAdapter databaseAdapter = createDatabaseAdapter();
      if (dataSource == null) {
        dataSource = createDataSource(databaseAdapter);
      }
      return new JdbcStore(cfg, dataSource, databaseAdapter);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void cleanup() {
    if (databaseContainer != null) {
      databaseContainer.stop();
      databaseContainer = null;
    }
    dataSource = null;
  }

  private DataSource createDataSource(DatabaseAdapter databaseAdapter) {
    String url = "jdbc:";
    switch (databaseAdapter.name()) {
      case "H2":
        // give it a name for H2, so it's not one-database-per-connection
        url = "jdbc:h2:mem:nessie";
        break;
      case "HSQL":
        url = "jdbc:hsqldb:mem:nessie";
        break;
      default:
        break;
    }
    String user = "";
    String pass = "";
    if (Boolean.getBoolean("it.nessie.store.jdbc.testcontainer")) {
      databaseContainer = startContainer(databaseAdapter);
      url = databaseContainer.getJdbcUrl();
      user = databaseContainer.getUsername();
      pass = databaseContainer.getPassword();
    }
    url = System.getProperty("it.nessie.store.jdbc.url", url);
    user = System.getProperty("it.nessie.store.jdbc.username", user);
    pass = System.getProperty("it.nessie.store.jdbc.password", pass);

    LOGGER.info("Creating data source, url={}, user={}", url, user);

    // We use the Agroal data-source/connection-pool here, just because Quarkus uses it.
    AgroalDataSourceConfigurationSupplier cfgSupplier = new AgroalDataSourceConfigurationSupplier();
    cfgSupplier
        .connectionPoolConfiguration()
        .initialSize(2)
        .maxSize(5)
        .connectionFactoryConfiguration()
        .jdbcUrl(url)
        .principal(new NamePrincipal(user))
        .credential(new SimplePassword(pass));
    return new io.agroal.pool.DataSource(cfgSupplier.get());
  }

  protected DatabaseAdapter createDatabaseAdapter() {
    String adapterName = System.getProperty("it.nessie.store.jdbc.databaseAdapter", "HSQL");
    if (adapterName == null || adapterName.isEmpty()) {
      throw new IllegalStateException("Must set system property it.nessie.store.jdbc.databaseAdapter");
    }

    try {
      return DatabaseAdapter.create(adapterName);
    } catch (Exception e) {
      try {
        @SuppressWarnings("unchecked") Class<DatabaseAdapter> clazz
            = (Class<DatabaseAdapter>) Class.forName("com.dremio.nessie.versioned.store.jdbc." + adapterName);
        return clazz.getDeclaredConstructor().newInstance();
      } catch (Exception e2) {
        throw new RuntimeException("Failed to instantiate DatabaseAdapter for " + adapterName
            + " (" + e + " / " + e2);
      }
    }
  }

  @Override
  public void close() {
    getStore().truncateTables();
    getStore().close();
  }

  abstract JdbcDatabaseContainer<?> startContainer(DatabaseAdapter databaseAdapter);
}

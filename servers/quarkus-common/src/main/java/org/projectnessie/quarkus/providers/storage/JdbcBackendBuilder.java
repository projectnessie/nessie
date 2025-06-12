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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.JDBC;

import io.quarkus.arc.All;
import io.quarkus.arc.InstanceHandle;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import io.quarkus.datasource.runtime.DataSourceBuildTimeConfig;
import io.quarkus.datasource.runtime.DataSourcesBuildTimeConfig;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.List;
import javax.sql.DataSource;
import org.projectnessie.quarkus.config.QuarkusJdbcConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendConfig;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
@StoreType(JDBC)
@Dependent
public class JdbcBackendBuilder implements BackendBuilder {

  /**
   * The name of the default datasource. Corresponds to the default map key in {@link
   * DataSourcesBuildTimeConfig#dataSources()}.
   */
  public static final String DEFAULT_DATA_SOURCE_NAME = "<default>";

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBackendBuilder.class);

  @Inject DataSourcesBuildTimeConfig dataSourcesConfig;

  @Inject
  @All
  @SuppressWarnings("CdiInjectionPointsInspection")
  List<InstanceHandle<DataSource>> dataSources;

  @Inject QuarkusJdbcConfig config;

  @PostConstruct
  public void checkDataSourcesConfiguration() {
    dataSourcesConfig.dataSources().forEach(this::checkDatabaseKind);
  }

  @Override
  public Backend buildBackend() {
    DataSource dataSource = selectDataSource();
    JdbcBackendConfig c =
        JdbcBackendConfig.builder()
            .datasourceName(config.datasourceName())
            .fetchSize(config.fetchSize())
            .dataSource(dataSource)
            .build();
    return new JdbcBackendFactory().buildBackend(c);
  }

  public static String unquoteDataSourceName(String dataSourceName) {
    if (dataSourceName.startsWith("\"") && dataSourceName.endsWith("\"")) {
      dataSourceName = dataSourceName.substring(1, dataSourceName.length() - 1);
    }
    return dataSourceName;
  }

  private void checkDatabaseKind(String dataSourceName, DataSourceBuildTimeConfig config) {
    if (config.dbKind().isEmpty()) {
      throw new IllegalArgumentException(
          "Database kind not configured for datasource " + dataSourceName);
    }
    String databaseKind = config.dbKind().get();
    if (!DatabaseKind.isPostgreSQL(databaseKind)
        && !DatabaseKind.isH2(databaseKind)
        && !DatabaseKind.isMariaDB(databaseKind)) {
      throw new IllegalArgumentException(
          "Database kind for datasource "
              + dataSourceName
              + " is configured to '"
              + databaseKind
              + "', which Nessie does not support yet; "
              + "currently PostgreSQL, H2, MariaDB (and MySQL via MariaDB driver) are supported. "
              + "Feel free to raise a pull request to support your database of choice.");
    }
  }

  private DataSource selectDataSource() {
    String dataSourceName =
        config
            .datasourceName()
            .map(JdbcBackendBuilder::unquoteDataSourceName)
            .orElse(DEFAULT_DATA_SOURCE_NAME);
    DataSource dataSource = findDataSourceByName(dataSourceName);
    if (dataSourceName.equals(DEFAULT_DATA_SOURCE_NAME)) {
      LOGGER.warn(
          "Using legacy datasource configuration under quarkus.datasource.*: "
              + "please migrate to quarkus.datasource.postgresql.* and "
              + "set nessie.version.store.persist.jdbc.datasource=postgresql");
    } else {
      LOGGER.info("Selected datasource: {}", dataSourceName);
    }
    return dataSource;
  }

  private DataSource findDataSourceByName(String dataSourceName) {
    for (InstanceHandle<DataSource> handle : dataSources) {
      String name = handle.getBean().getName();
      name = name == null ? DEFAULT_DATA_SOURCE_NAME : unquoteDataSourceName(name);
      if (name.equals(dataSourceName)) {
        return handle.get();
      }
    }
    throw new IllegalStateException("No datasource configured with name: " + dataSourceName);
  }
}

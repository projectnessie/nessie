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
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import java.util.List;
import javax.sql.DataSource;
import org.projectnessie.quarkus.config.QuarkusJdbcConfig;
import org.projectnessie.quarkus.config.datasource.DataSourceActivator;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendConfig;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StoreType(JDBC)
@Dependent
public class JdbcBackendBuilder implements BackendBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBackendBuilder.class);

  @Inject
  @All
  @SuppressWarnings("CdiInjectionPointsInspection")
  List<InstanceHandle<DataSource>> dataSources;

  @Inject QuarkusJdbcConfig config;

  @Override
  public Backend buildBackend() {
    JdbcBackendFactory factory = new JdbcBackendFactory();
    JdbcBackendConfig c =
        JdbcBackendConfig.builder().from(config).dataSource(selectDataSource()).build();
    return factory.buildBackend(c);
  }

  private DataSource selectDataSource() {
    String dataSourceName = config.datasource().map(DataSourceActivator::unquote).orElse("default");
    DataSource dataSource = null;
    for (InstanceHandle<DataSource> handle : dataSources) {
      String name = handle.getBean().getName();
      name = name == null ? "default" : DataSourceActivator.unquote(name);
      if (name.equals(dataSourceName)) {
        dataSource = handle.get();
      }
    }
    if (dataSource == null) {
      throw new IllegalStateException("No data source configured with name: " + dataSourceName);
    }
    if (dataSourceName.equals("default")) {
      LOGGER.warn(
          "Legacy datasource configuration found under quarkus.datasource.*: "
              + "please migrate to quarkus.datasource.postgresql.* and "
              + "set nessie.version.store.persist.jdbc.datasource=postgresql");
    } else {
      LOGGER.info("Using data source: {}", dataSourceName);
    }
    return dataSource;
  }
}

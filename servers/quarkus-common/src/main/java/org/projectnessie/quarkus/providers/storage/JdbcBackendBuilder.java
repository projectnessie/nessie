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

import io.agroal.api.AgroalDataSource;
import io.quarkus.datasource.common.runtime.DatabaseKind;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.quarkus.config.QuarkusJdbcConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendConfig;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendFactory;

@StoreType(JDBC)
@Dependent
public class JdbcBackendBuilder implements BackendBuilder {

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  AgroalDataSource dataSource;

  @Inject
  @ConfigProperty(name = "quarkus.datasource.db-kind")
  String databaseKind;

  @Inject QuarkusJdbcConfig config;

  @Override
  public Backend buildBackend() {
    if (!DatabaseKind.isPostgreSQL(databaseKind) && !DatabaseKind.isH2(databaseKind)) {
      throw new IllegalArgumentException(
        "Database kind is configured to '"
          + databaseKind
          + "', which Nessie does not support yet, PostgreSQL + H2 are supported. "
          + "Feel free to raise a pull request to support your database of choice.");
    }

    JdbcBackendFactory factory = new JdbcBackendFactory();
    JdbcBackendConfig c = JdbcBackendConfig.builder().from(config).dataSource(dataSource).build();
    return factory.buildBackend(c);
  }
}

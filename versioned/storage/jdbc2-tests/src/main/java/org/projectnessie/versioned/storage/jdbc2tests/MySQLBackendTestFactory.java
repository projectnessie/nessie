/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.jdbc2tests;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.projectnessie.versioned.storage.jdbc2.Jdbc2BackendFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

public class MySQLBackendTestFactory extends ContainerBackendTestFactory {

  @Override
  public String getName() {
    return Jdbc2BackendFactory.NAME + "-MySQL";
  }

  @Nonnull
  @Override
  protected JdbcDatabaseContainer<?> createContainer() {
    return new MariaDBDriverMySQLContainer(dockerImage("mysql"));
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of(
        "quarkus.datasource.mysql.jdbc.url",
        jdbcUrl(),
        "quarkus.datasource.mysql.username",
        jdbcUser(),
        "quarkus.datasource.mysql.password",
        jdbcPass());
  }

  private static class MariaDBDriverMySQLContainer
      extends MySQLContainer<MariaDBDriverMySQLContainer> {

    MariaDBDriverMySQLContainer(DockerImageName dockerImage) {
      super(dockerImage.asCompatibleSubstituteFor("mysql"));
      addParameter("TC_MY_CNF", "mysql-conf");
    }

    @Override
    public String getDriverClassName() {
      return "org.mariadb.jdbc.Driver";
    }

    @Override
    public String getJdbcUrl() {
      return super.getJdbcUrl().replace("jdbc:mysql", "jdbc:mariadb");
    }
  }
}

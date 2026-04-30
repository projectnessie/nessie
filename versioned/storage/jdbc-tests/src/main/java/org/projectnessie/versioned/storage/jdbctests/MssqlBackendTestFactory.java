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
package org.projectnessie.versioned.storage.jdbctests;

import jakarta.annotation.Nonnull;
import java.util.Map;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.mssqlserver.MSSQLServerContainer;

public class MssqlBackendTestFactory extends ContainerBackendTestFactory {

  @Override
  public String getName() {
    return JdbcBackendFactory.NAME + "-Mssql";
  }

  @Nonnull
  @Override
  @SuppressWarnings("resource")
  protected JdbcDatabaseContainer<?> createContainer() {
    return new MSSQLServerContainer(
            dockerImage("mssql").asCompatibleSubstituteFor("mcr.microsoft.com/mssql/server"))
        .acceptLicense();
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of(
        "quarkus.datasource.mssql.jdbc.url",
        jdbcUrl(),
        "quarkus.datasource.mssql.username",
        jdbcUser(),
        "quarkus.datasource.mssql.password",
        jdbcPass());
  }
}

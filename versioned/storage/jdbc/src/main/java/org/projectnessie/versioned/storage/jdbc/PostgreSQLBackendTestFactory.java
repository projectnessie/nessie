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
package org.projectnessie.versioned.storage.jdbc;

import jakarta.annotation.Nonnull;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgreSQLBackendTestFactory extends ContainerBackendTestFactory {

  @Override
  public String getName() {
    return JdbcBackendFactory.NAME + "-Postgres";
  }

  @Nonnull
  @Override
  protected JdbcDatabaseContainer<?> createContainer() {
    String version = System.getProperty("it.nessie.container.postgres.tag", "latest");
    return new PostgreSQLContainer<>("postgres:" + version);
  }
}

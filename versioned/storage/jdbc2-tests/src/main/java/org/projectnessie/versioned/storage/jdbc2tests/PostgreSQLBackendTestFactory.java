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
import java.time.Duration;
import java.util.Map;
import org.projectnessie.versioned.storage.jdbc2.Jdbc2BackendFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class PostgreSQLBackendTestFactory extends ContainerBackendTestFactory {

  @Override
  public String getName() {
    return Jdbc2BackendFactory.NAME + "-Postgres";
  }

  @Nonnull
  @Override
  protected JdbcDatabaseContainer<?> createContainer() {
    return new PostgreSQLContainer(dockerImage("postgres").asCompatibleSubstituteFor("postgres"))
        // use an age-based pull-policy, because image tags are just updated and there are no
        // "patch" version tags
        .withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)));
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of(
        "quarkus.datasource.postgresql.jdbc.url",
        jdbcUrl(),
        "quarkus.datasource.postgresql.username",
        jdbcUser(),
        "quarkus.datasource.postgresql.password",
        jdbcPass());
  }
}

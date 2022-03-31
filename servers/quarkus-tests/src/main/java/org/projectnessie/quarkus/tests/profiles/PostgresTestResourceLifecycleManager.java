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
package org.projectnessie.quarkus.tests.profiles;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
  private JdbcDatabaseContainer<?> container;

  @Override
  public Map<String, String> start() {
    String version = System.getProperty("it.nessie.container.postgres.tag", "9.6.22");
    container = new PostgreSQLContainer<>("postgres:" + version);

    try {
      // Only start the Docker container (local Dynamo-compatible). The DynamoDatabaseClient will
      // be configured via Quarkus -> Quarkus-Dynamo / DynamoVersionStoreFactory.
      container.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return ImmutableMap.of(
        "quarkus.datasource.username",
        container.getUsername(),
        "quarkus.datasource.password",
        container.getPassword(),
        "quarkus.datasource.jdbc.url",
        container.getJdbcUrl(),
        "quarkus.datasource.jdbc.extended-leak-report",
        "true");
  }

  @Override
  public void stop() {
    if (container != null) {
      try {
        container.stop();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        container = null;
      }
    }
  }
}

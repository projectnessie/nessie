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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class JdbcOssFixture extends JdbcFixture {
  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcOssFixture.class);

  JdbcDatabaseContainer<?> startContainer(DatabaseAdapter databaseAdapter) {
    try {
      JdbcDatabaseContainer<?> container;

      switch (databaseAdapter.name()) {
        case "Cockroach":
          container = new CockroachContainer("cockroachdb/cockroach:"
              // Note v20.2.x doesn't work
              + System.getProperty("it.nessie.store.jdbc.container-image-version", "v20.1.11"));
          break;
        case "PostgresQL":
          container = new PostgreSQLContainer<>("postgres:"
              + System.getProperty("it.nessie.store.jdbc.container-image-version", "9.6.12"));
          break;
        default:
          throw new IllegalArgumentException(
              "Dialect " + databaseAdapter.name() + " not supported for integration-tests");
      }

      container.withLogConsumer(new Slf4jLogConsumer(LOGGER))
          .start();

      return container;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}

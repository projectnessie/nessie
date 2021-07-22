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
package org.projectnessie.versioned.tiered.postgres;

import java.util.Locale;
import org.projectnessie.versioned.tiered.tx.TxDatabaseAdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;

final class ContainerFixture {
  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerFixture.class);

  static JdbcDatabaseContainer<?> container;

  private final String imageName;
  private final String version;

  ContainerFixture(String imageName, String version, String versionPropertyName) {
    this.imageName = imageName;
    this.version = System.getProperty(versionPropertyName, version);
  }

  void setup() {
    if (imageName.toLowerCase(Locale.ROOT).contains("postgres")) {
      container = new PostgreSQLContainer(imageName + ":" + version);
    }
    if (imageName.toLowerCase(Locale.ROOT).contains("cockroach")) {
      container = new CockroachContainer(imageName + ":" + version);
    }
    container.withLogConsumer(new Slf4jLogConsumer(LOGGER)).start();
  }

  void stop() {
    try {
      container.stop();
    } finally {
      container = null;
    }
  }

  TxDatabaseAdapterConfig configureDatabaseAdapter(TxDatabaseAdapterConfig config) {
    return config
        .withJdbcUrl(container.getJdbcUrl())
        .withJdbcUser(container.getUsername())
        .withJdbcPass(container.getPassword());
  }
}

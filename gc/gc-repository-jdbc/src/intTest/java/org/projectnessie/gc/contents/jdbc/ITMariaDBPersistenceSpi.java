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
package org.projectnessie.gc.contents.jdbc;

import static org.projectnessie.gc.contents.jdbc.ITPostgresPersistenceSpi.dockerImage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.mariadb.MariaDBContainer;

public class ITMariaDBPersistenceSpi extends AbstractJdbcPersistenceSpi {

  private static MariaDBContainer container;

  @BeforeAll
  static void createDataSource() throws Exception {
    container = new MariaDBContainer(dockerImage("mariadb").asCompatibleSubstituteFor("mariadb"));
    container.start();
    initDataSource(container.getJdbcUrl());
  }

  @AfterAll
  static void stopContainer() {
    if (container != null) {
      container.stop();
    }
  }
}

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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.projectnessie.versioned.tiered.tests.AbstractTieredCommitsTest;
import org.projectnessie.versioned.tiered.tx.TxDatabaseAdapterConfig;

@EnabledIfSystemProperty(named = "it.nessie.dbs", matches = ".*postgres.*")
public class ITTieredCommitsPostgres extends AbstractTieredCommitsTest<TxDatabaseAdapterConfig> {
  static ContainerFixture container =
      new ContainerFixture("postgres", "9.6.22", "it.nessie.container.postgres.tag");

  @BeforeAll
  static void startContainer() {
    container.setup();
  }

  @AfterAll
  static void stopContainer() {
    container.stop();
  }

  @Override
  protected TxDatabaseAdapterConfig configureDatabaseAdapter(TxDatabaseAdapterConfig config) {
    return container.configureDatabaseAdapter(config);
  }
}

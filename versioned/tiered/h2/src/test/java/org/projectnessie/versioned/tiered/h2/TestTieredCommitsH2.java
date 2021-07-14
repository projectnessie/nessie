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
package org.projectnessie.versioned.tiered.h2;

import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration;
import org.projectnessie.versioned.tiered.tests.TestTieredCommits;
import org.projectnessie.versioned.tiered.tx.TxConnectionProvider.LocalConnectionProvider;

public class TestTieredCommitsH2 extends TestTieredCommits {

  @Override
  protected void configureDatabaseAdapter(DatabaseAdapterConfiguration config) {
    LocalConnectionProvider.JDBC_URL.set(config, "jdbc:h2:mem:nessie");
  }

  @Override
  protected void postCloseDatabaseAdapter() {
    ((H2DatabaseAdapter) databaseAdapter).dropTables();
  }
}

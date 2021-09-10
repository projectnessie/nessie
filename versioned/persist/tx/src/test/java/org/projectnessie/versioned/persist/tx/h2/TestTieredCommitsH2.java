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
package org.projectnessie.versioned.persist.tx.h2;

import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.tests.AbstractTieredCommitsTest;
import org.projectnessie.versioned.persist.tx.local.ImmutableDefaultLocalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tx.local.LocalDatabaseAdapterConfig;

public class TestTieredCommitsH2 extends AbstractTieredCommitsTest {

  @BeforeAll
  static void configureAdapter() {
    createAdapter(
        DatabaseAdapterFactory.<LocalDatabaseAdapterConfig>loadFactoryByName("H2")
            .newBuilder()
            .withConfig(
                ImmutableDefaultLocalDatabaseAdapterConfig.builder()
                    .jdbcUrl("jdbc:h2:mem:nessie")
                    .build()),
        c -> c);
  }
}

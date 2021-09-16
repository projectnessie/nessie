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
package org.projectnessie.versioned.persist.mongodb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.persist.tests.AbstractTieredCommitsTest;

@ExtendWith(MongoDatabaseAdapterExtension.class)
public class TestMongoDatabaseAdapter extends AbstractTieredCommitsTest {

  @Test
  void testClientFromConfig() {
    MongoDatabaseAdapterConfig config = ((MongoDatabaseAdapter) databaseAdapter).getConfig();

    MongoDatabaseClient client = new MongoDatabaseClient(config);
    assertThat(client.acquired()).isEqualTo(0);

    MongoDatabaseAdapter adapter = new MongoDatabaseAdapter(config.withClient(client));
    assertThat(client.acquired()).isEqualTo(1);

    adapter.close();
    assertThat(client.acquired()).isEqualTo(0);
  }
}

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
package org.projectnessie.versioned.tiered.rocks;

import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.versioned.tiered.tests.AbstractVersionStoreTest;

public class TestVersionStoreRocks extends AbstractVersionStoreTest<RocksDatabaseAdapterConfig> {
  @TempDir static Path rocksDir;

  static RocksDbInstance instance;

  @Override
  protected RocksDatabaseAdapterConfig configureDatabaseAdapter(RocksDatabaseAdapterConfig config) {
    if (instance == null) {
      instance = new RocksDbInstance();
      instance.setDbPath(rocksDir.toString());
    }

    return ImmutableRocksDatabaseAdapterConfig.builder().from(config).dbInstance(instance).build();
  }

  @AfterAll
  static void closeRocks() {
    instance.close();
  }
}

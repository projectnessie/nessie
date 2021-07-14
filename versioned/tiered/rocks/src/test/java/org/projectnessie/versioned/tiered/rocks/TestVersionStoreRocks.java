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
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.versioned.tiered.adapter.DatabaseAdapterConfiguration;
import org.projectnessie.versioned.tiered.tests.TestVersionStore;

public class TestVersionStoreRocks extends TestVersionStore {
  @TempDir Path rocksDir;

  RocksDbInstance instance;

  @Override
  protected void configureDatabaseAdapter(DatabaseAdapterConfiguration config) throws Exception {
    instance = new RocksDbInstance();
    instance.setDbPath(rocksDir);

    RocksDatabaseAdapter.ROCKS_DB.set(config, instance);
  }

  @Override
  protected void postCloseDatabaseAdapter() {
    instance.close();
  }
}

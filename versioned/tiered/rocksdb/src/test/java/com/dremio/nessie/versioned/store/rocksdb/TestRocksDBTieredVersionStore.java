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
package com.dremio.nessie.versioned.store.rocksdb;

import java.nio.file.Path;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.versioned.impl.AbstractITTieredVersionStore;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled("RocksDBStore not fully implemented")
class TestRocksDBTieredVersionStore extends AbstractITTieredVersionStore {
  @TempDir
  static Path DB_PATH;

  @Override
  protected RocksDBStoreFixture createNewFixture() {
    return new RocksDBStoreFixture(DB_PATH.toString());
  }
}

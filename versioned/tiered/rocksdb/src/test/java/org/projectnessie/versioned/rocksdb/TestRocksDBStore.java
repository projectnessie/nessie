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
package org.projectnessie.versioned.rocksdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import org.projectnessie.versioned.impl.AbstractTestStore;
import com.google.common.collect.ImmutableList;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestRocksDBStore extends AbstractTestStore<RocksDBStore> {
  @TempDir
  static Path DB_PATH;

  @AfterAll
  static void tearDown() throws IOException {
    for (Path path : ImmutableList.of(DB_PATH, getRawPath())) {
      if (path.toFile().exists()) {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
  }

  /**
   * Creates an instance of RocksDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected RocksDBStore createStore() {
    return new RocksDBStore(createConfig(DB_PATH.toString()));
  }

  @Override
  protected RocksDBStore createRawStore() {
    // Need to use a separate path to avoid reusing the file created by the normal store.
    return new RocksDBStore(createConfig(getRawPath().toString()));
  }

  @Override
  protected int loadSize() {
    // There is no pagination in RocksDB.
    return Integer.MAX_VALUE;
  }

  @Override
  protected long getRandomSeed() {
    return -2938423452345L;
  }

  @Override
  protected void resetStoreState() {
    store.deleteAllData();
  }

  private RocksDBStoreConfig createConfig(String path) {
    return new RocksDBStoreConfig() {
      public String getDbDirectory() {
        return path;
      }
    };
  }

  private static Path getRawPath() {
    return DB_PATH.resolve("raw");
  }

  @Override
  protected boolean supportsUpdate() {
    return false;
  }
}

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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.versioned.impl.AbstractTestStore;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import com.google.common.collect.ImmutableList;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestRocksDBStore extends AbstractTestStore<TestRocksDBStore.RocksDBStoreUUT> {
  @TempDir
  static Path DB_PATH;

  @AfterAll
  static void tearDown() throws IOException {
    for (Path path : ImmutableList.of(DB_PATH, getRawPath())) {
      if (Files.exists(path)) {
        final List<Path> pathList = Files.walk(path).sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        for (Path pathToDelete : pathList) {
          Files.delete(pathToDelete);
        }
      }
    }
  }

  /**
   * Creates an instance of RocksDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected RocksDBStoreUUT createStore() {
    return new RocksDBStoreUUT(createConfig(DB_PATH.toString()));
  }

  @Override
  protected RocksDBStoreUUT createRawStore() {
    // Need to use a separate path to avoid reusing the file created by the normal store.
    return new RocksDBStoreUUT(createConfig(getRawPath().toString()));
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

  /**
   * A Unit Under Test version of RocksDBStore that provides the facility to delete all column families.
   */
  static class RocksDBStoreUUT extends RocksDBStore {
    /**
     * Creates a store ready for connection to RocksDB.
     * @param config the configuration for the store.
     */
    public RocksDBStoreUUT(RocksDBStoreConfig config) {
      super(config);
    }

    /**
     * Delete all the data in all column families, used for testing only.
     */
    void deleteAllData() {
      // RocksDB doesn't expose a way to get the min/max key for a column family, so just use the min/max possible.
      byte[] minId = new byte[20];
      byte[] maxId = new byte[20];
      Arrays.fill(minId, (byte)0);
      Arrays.fill(maxId, (byte)255);

      for (ColumnFamilyHandle handle : valueTypeToColumnFamily.values()) {
        try {
          rocksDB.deleteRange(handle, minId, maxId);
          // Since RocksDB#deleteRange() is exclusive of the max key, delete it to ensure the column family is empty.
          rocksDB.delete(maxId);
        } catch (RocksDBException e) {
          throw new RuntimeException(e);
        }
      }
    }

  }
}

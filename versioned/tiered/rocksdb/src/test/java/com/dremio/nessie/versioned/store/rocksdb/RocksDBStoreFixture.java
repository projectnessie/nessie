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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

import com.dremio.nessie.versioned.impl.AbstractTieredStoreFixture;

/**
 * RocksDB Store fixture.
 *
 * <p>Combine a local RocksDB database with a {@code VersionStore} instance to be used for tests.
 */
public class RocksDBStoreFixture extends AbstractTieredStoreFixture<RocksDBStore, RocksDBStoreConfig> {

  /**
   * Create the RocksDB store-fixture.
   *
   * @param dbPath RocksDB database path
   */
  public RocksDBStoreFixture(String dbPath) {
    super(new RocksDBStoreConfig() {
      @Override
      public String getDbDirectory() {
        return dbPath;
      }
    });
  }

  public RocksDBStore createStoreImpl() {
    return new RocksDBStore(getConfig());
  }

  @Override
  public void close() {
    getStore().deleteAllData();
    getStore().close();

    final Path path = Paths.get(getConfig().getDbDirectory());
    if (path.toFile().exists()) {
      try {
        Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      } catch (IOException e) {
        // Ignore this since we're closing.
      }
    }
  }
}

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
import java.util.Comparator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;

import com.dremio.nessie.versioned.tests.AbstractTestStore;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestRocksDBStore extends AbstractTestStore<RocksDBStore> {
  @TempDir
  static Path DB_PATH;

  @AfterAll
  static void tearDown() throws IOException {
    if (DB_PATH.toFile().exists()) {
      Files.walk(DB_PATH).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  /**
   * Creates an instance of RocksDBStore on which tests are executed.
   * @return the store to test.
   */
  @Override
  protected RocksDBStore createStore() {
    return new RocksDBStore(DB_PATH.toString());
  }

  @Override
  protected long getRandomSeed() {
    return -2938423452345L;
  }

  @Override
  protected void resetStoreState() {
    store.deleteAllData();
  }

  @Override
  public void putIfAbsentBranch() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentBranch());
  }

  @Override
  public void putIfAbsentCommitMetadata() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentCommitMetadata());
  }

  @Override
  public void putIfAbsentFragment() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentFragment());
  }

  @Override
  public void putIfAbsentL1() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentL1());
  }

  @Override
  public void putIfAbsentL2() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentL2());
  }

  @Override
  public void putIfAbsentL3() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentL3());
  }

  @Override
  public void putIfAbsentTag() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentTag());
  }

  @Override
  public void putIfAbsentValue() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> super.putIfAbsentValue());
  }
}

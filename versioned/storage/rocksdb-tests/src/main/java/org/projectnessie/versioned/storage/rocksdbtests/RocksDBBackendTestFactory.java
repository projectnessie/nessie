/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.rocksdbtests;

import static java.nio.file.FileVisitResult.CONTINUE;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.rocksdb.RocksDBBackend;
import org.projectnessie.versioned.storage.rocksdb.RocksDBBackendConfig;
import org.projectnessie.versioned.storage.rocksdb.RocksDBBackendFactory;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;

public final class RocksDBBackendTestFactory implements BackendTestFactory {

  private Path rocksDir;

  private RocksDBBackend backend;

  @Override
  public Backend createNewBackend() {
    return backend;
  }

  @Override
  public String getName() {
    return RocksDBBackendFactory.NAME;
  }

  @Override
  public void start() throws Exception {
    rocksDir = Files.createTempDirectory("junit-nessie-rocksdb");

    RocksDBBackendConfig config = RocksDBBackendConfig.builder().databasePath(rocksDir).build();
    backend = new RocksDBBackend(config);
  }

  @Override
  public void stop() throws Exception {
    RocksDBBackend b = backend;
    Path dir = rocksDir;
    backend = null;
    rocksDir = null;
    try {
      if (b != null) {
        b.close();
      }
    } finally {
      if (dir != null) {
        deleteTempDir(dir);
      }
    }
  }

  private static void deleteTempDir(Path dir) throws IOException {
    if (Files.notExists(dir)) {
      return;
    }

    List<IOException> failures = new ArrayList<>();
    Files.walkFileTree(
        dir,
        new SimpleFileVisitor<>() {

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            return tryDelete(file);
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
            return tryDelete(dir);
          }

          private FileVisitResult tryDelete(Path path) {
            try {
              Files.delete(path);
            } catch (NoSuchFileException ignore) {
              // pass
            } catch (IOException e) {
              failures.add(e);
            }
            return CONTINUE;
          }
        });

    if (!failures.isEmpty()) {
      IOException e = new IOException("Could not delete temp-directory " + dir);
      failures.forEach(e::addSuppressed);
      throw e;
    }
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of("nessie.version.store.persist.rocks.database-path", rocksDir.toString());
  }
}

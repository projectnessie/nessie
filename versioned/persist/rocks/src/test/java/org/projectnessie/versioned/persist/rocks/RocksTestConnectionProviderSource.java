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
package org.projectnessie.versioned.persist.rocks;

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
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.TestConnectionProviderSource;

public class RocksTestConnectionProviderSource
    implements TestConnectionProviderSource<RocksDbInstance> {

  private Path rocksDir;
  private RocksDbInstance instance;

  @Override
  public void start() throws Exception {
    rocksDir = Files.createTempDirectory("junit-rocks");
    instance = new RocksDbInstance();
    instance.configure(ImmutableRocksDbConfig.builder().dbPath(rocksDir.toString()).build());
    instance.initialize();
  }

  @Override
  public void stop() throws Exception {
    try {
      if (instance != null) {
        instance.close();
      }
    } finally {
      instance = null;
      if (rocksDir != null) {
        deleteTempDir(rocksDir);
      }
    }
  }

  @Override
  public DatabaseAdapterConfig<RocksDbInstance> updateConfig(
      DatabaseAdapterConfig<RocksDbInstance> config) {
    return config.withConnectionProvider(instance);
  }

  private static void deleteTempDir(Path dir) throws IOException {
    if (Files.notExists(dir)) {
      return;
    }

    List<IOException> failures = new ArrayList<>();
    Files.walkFileTree(
        dir,
        new SimpleFileVisitor<Path>() {

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
}

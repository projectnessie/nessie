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
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterFactory;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.tests.extension.AbstractTestConnectionProviderSource;

/**
 * RocksDB test connection-provider source.
 *
 * <p>See {@link RocksDbConfig} for configuration options.
 */
public class RocksTestConnectionProviderSource
    extends AbstractTestConnectionProviderSource<RocksDbConfig> {

  private Path rocksDir;

  @Override
  public boolean isCompatibleWith(
      DatabaseAdapterConfig adapterConfig, DatabaseAdapterFactory<?, ?, ?> databaseAdapterFactory) {
    return adapterConfig instanceof NonTransactionalDatabaseAdapterConfig
        && databaseAdapterFactory instanceof RocksDatabaseAdapterFactory;
  }

  @Override
  public RocksDbConfig createDefaultConnectionProviderConfig() {
    return ImmutableRocksDbConfig.builder().build();
  }

  @Override
  public RocksDbInstance createConnectionProvider() {
    return new RocksDbInstance();
  }

  @Override
  public void start() throws Exception {
    rocksDir = Files.createTempDirectory("junit-rocks");
    configureConnectionProviderConfigFromDefaults(c -> c.withDbPath(rocksDir.toString()));
    super.start();
  }

  @Override
  public void stop() throws Exception {
    try {
      super.stop();
    } finally {
      if (rocksDir != null) {
        deleteTempDir(rocksDir);
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

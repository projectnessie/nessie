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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.tests.DatabaseAdapterExtension;

public class RocksDatabaseAdapterExtension extends DatabaseAdapterExtension {
  private static final Namespace NAMESPACE = Namespace.create(RocksDatabaseAdapterExtension.class);

  public RocksDatabaseAdapterExtension() {}

  @Override
  protected DatabaseAdapter createAdapter(ExtensionContext context, TestConfigurer testConfigurer) {
    Path rocksDir;
    try {
      rocksDir = Files.createTempDirectory("junit-rocks");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    RocksDbInstance instance = new RocksDbInstance();
    context.getStore(NAMESPACE).put("mongod", new RocksResource(rocksDir, instance));

    instance.setDbPath(rocksDir.toString());

    return createAdapter(
        "RocksDB",
        testConfigurer,
        config ->
            ImmutableRocksDatabaseAdapterConfig.builder()
                .from(config)
                .dbInstance(instance)
                .build());
  }

  static class RocksResource implements CloseableResource {
    private final Path rocksDir;
    private final RocksDbInstance instance;

    RocksResource(Path rocksDir, RocksDbInstance instance) {
      this.rocksDir = rocksDir;
      this.instance = instance;
    }

    @Override
    public void close() throws IOException {

      try {
        if (instance != null) {
          instance.close();
        }
      } finally {

        if (rocksDir != null) {
          deleteTempDir(rocksDir);
        }
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

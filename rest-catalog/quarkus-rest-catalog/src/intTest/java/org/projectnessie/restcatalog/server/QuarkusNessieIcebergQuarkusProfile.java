/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.restcatalog.server;

import static java.lang.String.format;
import static java.nio.file.FileVisitResult.CONTINUE;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.junit.QuarkusTestProfile;
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

public class QuarkusNessieIcebergQuarkusProfile implements QuarkusTestProfile {

  @Override
  public Map<String, String> getConfigOverrides() {
    String uri =
        format(
            "%s/api/v2",
            requireNonNull(
                System.getProperty("nessie.it.http.url"),
                "Required system property nessie.it.http.url is not set"));

    return ImmutableMap.of(
        "nessie.iceberg.nessie-client.\"nessie.uri\"",
        uri,
        // TODO enable auth in ITs
        "nessie.iceberg.nessie-client.\"nessie.authentication.type\"",
        "NONE",
        "quarkus.oidc.enabled",
        "false");
  }

  @Override
  public List<TestResourceEntry> testResources() {
    return ImmutableList.of(new TestResourceEntry(WarehouseTestLocation.class));
  }

  public static class WarehouseTestLocation implements QuarkusTestResourceLifecycleManager {
    private Path warehouseDir;

    @Override
    public Map<String, String> start() {
      try {
        warehouseDir = Files.createTempDirectory("test-quarkus-nessie-iceberg-warehouse");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return ImmutableMap.of("nessie.iceberg.warehouseLocation", warehouseDir.toUri().toString());
    }

    @Override
    public void stop() {
      try {
        deleteTempDir(warehouseDir);
      } catch (Exception e) {
        throw new IllegalStateException(e);
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
  }
}

/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.internal;

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
import java.util.Objects;

/**
 * Cannot use JUnit's {@code @TempDir} annotation, because JUnit extensions cannot depend on other
 * extensions.
 */
final class TemporaryDirectory implements AutoCloseable {
  TemporaryDirectory() {}

  private Path path;

  synchronized Path getPath() {
    if (path == null) {
      try {
        path = Files.createTempDirectory("junit-rocks");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return path;
  }

  @Override
  public void close() throws Exception {
    if (Files.notExists(path)) {
      return;
    }

    List<IOException> failures = new ArrayList<>();
    Files.walkFileTree(
        path,
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
      IOException e = new IOException("Could not delete temp-directory " + path);
      failures.forEach(e::addSuppressed);
      throw e;
    }
  }

  @Override
  public String toString() {
    return "TemporaryDirectory{" + "path=" + path + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TemporaryDirectory that = (TemporaryDirectory) o;
    return Objects.equals(path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }
}

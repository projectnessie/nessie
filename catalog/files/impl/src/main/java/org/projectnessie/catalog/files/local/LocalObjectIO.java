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
package org.projectnessie.catalog.files.local;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.storage.uri.StorageUri;

/** An {@link ObjectIO} implementation purely for unit tests - and nothing else. */
public class LocalObjectIO implements ObjectIO {
  @Override
  public void ping(StorageUri uri) throws IOException {
    Path path = filePath(uri);
    if (!Files.isDirectory(path)) {
      throw new FileNotFoundException(path.toString());
    }
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    return Files.newInputStream(filePath(uri));
  }

  @Override
  public OutputStream writeObject(StorageUri uri) throws IOException {
    try {
      Path path = filePath(uri);
      Files.createDirectories(path.getParent());
      return Files.newOutputStream(path);
    } catch (FileSystemNotFoundException e) {
      throw new UnsupportedOperationException(
          "Writing to " + uri.scheme() + " URIs is not supported", e);
    }
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) throws IOException {
    IOException ex = null;
    for (StorageUri uri : uris) {
      Path path = LocalObjectIO.filePath(uri);
      try {
        Files.deleteIfExists(path);
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        } else {
          ex.addSuppressed(e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public void configureIcebergWarehouse(
      StorageUri warehouse,
      BiConsumer<String, String> defaultConfig,
      BiConsumer<String, String> configOverride) {}

  @Override
  public void configureIcebergTable(
      StorageLocations storageLocations,
      BiConsumer<String, String> config,
      Predicate<Duration> enableRequestSigning,
      boolean canDoCredentialsVending) {}

  @Override
  public void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties) {}

  @Override
  public Optional<String> canResolve(StorageUri uri) {
    Path path = LocalObjectIO.filePath(uri);
    // We expect a directory here (or non-existing here), because the URI is meant to store other
    // files
    return Files.isRegularFile(path)
        ? Optional.of(path + " does must not be file")
        : Optional.empty();
  }

  private static Path filePath(StorageUri uri) {
    return Paths.get(uri.requiredPath());
  }
}

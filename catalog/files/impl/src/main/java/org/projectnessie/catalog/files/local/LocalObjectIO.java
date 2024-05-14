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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.storage.uri.StorageUri;

/** An {@link ObjectIO} implementation purely for unit tests - and nothing else. */
public class LocalObjectIO implements ObjectIO {
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

  private static Path filePath(StorageUri uri) {
    return Paths.get(uri.requiredPath());
  }

  @Override
  public boolean isValidUri(StorageUri uri) {
    return uri != null && ("file".equals(uri.scheme()) || uri.scheme() == null);
  }
}

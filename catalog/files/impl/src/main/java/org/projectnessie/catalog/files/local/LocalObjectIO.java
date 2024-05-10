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
import java.net.URI;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.projectnessie.catalog.files.api.ObjectIO;

/** An {@link ObjectIO} implementation purely for unit tests - and nothing else. */
public class LocalObjectIO implements ObjectIO {
  @Override
  public InputStream readObject(URI uri) throws IOException {
    return fileUri(uri).toURL().openStream();
  }

  @Override
  public OutputStream writeObject(URI uri) throws IOException {
    try {
      Path path = Paths.get(fileUri(uri));
      Files.createDirectories(path.getParent());
      return Files.newOutputStream(path);
    } catch (FileSystemNotFoundException e) {
      throw new UnsupportedOperationException(
          "Writing to " + uri.getScheme() + " URIs is not supported", e);
    }
  }

  private static URI fileUri(URI uri) {
    if (uri.getScheme() == null) {
      uri = Paths.get(uri.getPath()).toUri();
    }
    return uri;
  }

  @Override
  public boolean isValidUri(URI uri) {
    return uri != null && ("file".equals(uri.getScheme()) || uri.getScheme() == null);
  }
}

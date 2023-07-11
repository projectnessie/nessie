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
package org.projectnessie.catalog.service.testing;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class LocalFileIO implements FileIO {

  @SuppressWarnings("unused")
  public LocalFileIO() {}

  @Override
  public InputFile newInputFile(String path) {
    return Files.localInput(asPath(path).toFile());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return Files.localOutput(asPath(path).toFile());
  }

  @Override
  public void deleteFile(String path) {
    try {
      java.nio.file.Files.delete(asPath(path));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Path asPath(String path) {
    URI uri = URI.create(path);
    if (uri.getScheme() == null) {
      return Paths.get(uri.getPath());
    }
    return Paths.get(uri);
  }
}

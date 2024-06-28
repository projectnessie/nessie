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
package org.projectnessie.gc.files.local;

import static org.projectnessie.storage.uri.StorageUri.SCHEME_FILE;

import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.util.AbstractMap;
import java.util.Objects;
import java.util.stream.Stream;
import org.projectnessie.gc.files.DeleteResult;
import org.projectnessie.gc.files.FileDeleter;
import org.projectnessie.gc.files.FileReference;
import org.projectnessie.gc.files.FilesLister;
import org.projectnessie.gc.files.NessieFileIOException;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link FileDeleter} and {@link FilesLister} with direct access to files on the
 * local file system.
 */
public class LocalFiles implements FilesLister, FileDeleter {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFiles.class);

  private Path toLocalPath(StorageUri uri) {
    Preconditions.checkArgument(SCHEME_FILE.equals(uri.scheme()), "Not a local file: %s", uri);
    return Paths.get(URI.create(Objects.requireNonNull(uri.location())));
  }

  @SuppressWarnings("resource")
  @Override
  @MustBeClosed
  public Stream<FileReference> listRecursively(StorageUri path) throws NessieFileIOException {
    // .withTrailingSeparator() is required for proper .relativize() behaviour
    StorageUri basePath = path.withTrailingSeparator();
    try {
      Path start = toLocalPath(basePath);
      return Files.walk(start)
          .filter(p -> !p.equals(start))
          .map(
              p -> {
                try {
                  return new AbstractMap.SimpleEntry<>(
                      p.toUri(),
                      Files.getFileAttributeView(p, BasicFileAttributeView.class).readAttributes());
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              })
          .filter(e -> e.getValue().isRegularFile())
          .map(
              e ->
                  FileReference.of(
                      basePath.relativize(StorageUri.of(e.getKey())),
                      basePath,
                      e.getValue().lastModifiedTime().toMillis()));
    } catch (IOException e) {
      throw new NessieFileIOException("Failed to list files in " + path, e);
    }
  }

  @Override
  public DeleteResult delete(FileReference fileReference) {
    try {
      Files.delete(toLocalPath(fileReference.absolutePath()));
      return DeleteResult.SUCCESS;
    } catch (NoSuchFileException e) {
      return DeleteResult.SUCCESS;
    } catch (IOException e) {
      LOGGER.debug("Failed to delete {}", fileReference, e);
      return DeleteResult.FAILURE;
    }
  }
}

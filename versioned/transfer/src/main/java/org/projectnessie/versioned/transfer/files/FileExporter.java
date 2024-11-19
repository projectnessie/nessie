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
package org.projectnessie.versioned.transfer.files;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.newOutputStream;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.immutables.value.Value;

/** Nessie exporter that writes to individual files into an empty target directory. */
@Value.Immutable
public abstract class FileExporter implements ExportFileSupplier {

  public static Builder builder() {
    return ImmutableFileExporter.builder();
  }

  public interface Builder {
    Builder targetDirectory(Path targetDirectory);

    FileExporter build();
  }

  abstract Path targetDirectory();

  @Override
  public long fixMaxFileSize(long userProvidedMaxFileSize) {
    return userProvidedMaxFileSize;
  }

  @Override
  @Nonnull
  public Path getTargetPath() {
    return targetDirectory();
  }

  @Override
  public void preValidate() throws IOException {
    if (isDirectory(targetDirectory())) {
      try (Stream<Path> listing = Files.list(targetDirectory())) {
        checkState(
            listing.findAny().isEmpty(),
            "Target directory %s must be empty, but is not",
            targetDirectory());
      }
    } else {
      createDirectories(targetDirectory());
    }
  }

  @Override
  @Nonnull
  public OutputStream newFileOutput(@Nonnull String fileName) throws IOException {
    checkArgument(
        fileName.indexOf('/') == -1 && fileName.indexOf('\\') == -1, "Directories not supported");
    Path f = Paths.get(fileName);
    checkArgument(!fileName.isEmpty(), "Invalid file name argument");

    f = targetDirectory().resolve(f);
    checkState(!exists(f), "File %s already exists", fileName);
    return newOutputStream(f);
  }

  @Override
  public void close() {}
}

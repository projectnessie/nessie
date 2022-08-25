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
package org.projectnessie.versioned.transfer;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.immutables.value.Value;

/** Nessie exporter that writes to individual files into an empty target directory. */
@Value.Immutable
public abstract class FileExporter extends AbstractNessieExporter {

  public static Builder builder() {
    return ImmutableFileExporter.builder();
  }

  public interface Builder
      extends AbstractNessieExporter.Builder<FileExporter.Builder, FileExporter> {
    Builder targetDirectory(Path targetDirectory);
  }

  abstract Path targetDirectory();

  @Override
  protected void preValidate() throws IOException {
    if (Files.isDirectory(targetDirectory())) {
      try (Stream<Path> listing = Files.list(targetDirectory())) {
        Preconditions.checkState(
            !listing.findAny().isPresent(),
            "Target directory %s must be empty, but is not",
            targetDirectory());
      }
    } else {
      Files.createDirectories(targetDirectory());
    }
  }

  @Override
  protected OutputStream newFileOutput(String fileName) throws IOException {
    return Files.newOutputStream(targetDirectory().resolve(fileName));
  }
}

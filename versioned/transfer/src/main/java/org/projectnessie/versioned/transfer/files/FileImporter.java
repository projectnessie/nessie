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

import static java.nio.file.Files.newInputStream;
import static org.projectnessie.versioned.transfer.ExportImportConstants.DEFAULT_BUFFER_SIZE;

import jakarta.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import org.immutables.value.Value;
import org.projectnessie.versioned.transfer.ExportImportConstants;

/** Nessie importer using data from directory. */
@Value.Immutable
public abstract class FileImporter implements ImportFileSupplier {
  public static Builder builder() {
    return ImmutableFileImporter.builder();
  }

  public interface Builder {
    Builder sourceDirectory(Path sourceDirectory);

    /**
     * Optional, specify a different buffer size than the default value of {@value
     * ExportImportConstants#DEFAULT_BUFFER_SIZE}.
     */
    Builder inputBufferSize(int inputBufferSize);

    FileImporter build();
  }

  @Value.Default
  int inputBufferSize() {
    return DEFAULT_BUFFER_SIZE;
  }

  abstract Path sourceDirectory();

  @Override
  @Nonnull
  public InputStream newFileInput(@Nonnull String fileName) throws IOException {
    return new BufferedInputStream(
        newInputStream(sourceDirectory().resolve(fileName)), inputBufferSize());
  }

  @Override
  public void close() {}
}

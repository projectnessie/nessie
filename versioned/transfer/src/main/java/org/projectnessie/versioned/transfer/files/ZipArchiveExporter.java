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
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.isRegularFile;
import static java.nio.file.Files.move;
import static java.nio.file.Files.newOutputStream;

import jakarta.annotation.Nonnull;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.immutables.value.Value;

/** Nessie exporter that creates a ZIP file. */
@Value.Immutable
public abstract class ZipArchiveExporter implements ExportFileSupplier {

  public static Builder builder() {
    return ImmutableZipArchiveExporter.builder();
  }

  public interface Builder {
    Builder outputFile(Path outputFile);

    ZipArchiveExporter build();
  }

  abstract Path outputFile();

  @Value.Lazy
  Path tempOutputFile() {
    Path out = outputFile();
    return out.resolveSibling("." + out.getFileName().toString() + ".tmp");
  }

  @Value.Lazy
  ZipOutputStream zipOutput() throws IOException {
    Path parent = outputFile().getParent();
    if (parent != null) {
      createDirectories(parent);
    }
    return new ZipOutputStream(new BufferedOutputStream(newOutputStream(tempOutputFile())));
  }

  @Override
  public void preValidate() {}

  @Override
  @Nonnull
  public Path getTargetPath() {
    return outputFile();
  }

  @Override
  @Nonnull
  public OutputStream newFileOutput(@Nonnull String fileName) throws IOException {
    checkArgument(
        fileName.indexOf('/') == -1 && fileName.indexOf('\\') == -1, "Directories not supported");
    checkArgument(!fileName.isEmpty(), "Invalid file name argument");

    ZipOutputStream out = zipOutput();
    out.putNextEntry(new ZipEntry(fileName));
    return new NonClosingOutputStream(out);
  }

  @Override
  public void close() throws Exception {
    try {
      zipOutput().close();
    } finally {
      deleteIfExists(outputFile());
      if (isRegularFile(tempOutputFile())) {
        move(tempOutputFile(), outputFile());
      }
    }
  }

  private static final class NonClosingOutputStream extends OutputStream {
    private final ZipOutputStream out;
    private boolean open = true;

    private NonClosingOutputStream(ZipOutputStream out) {
      this.out = out;
    }

    @Override
    public void write(byte[] b) throws IOException {
      checkState(open);
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      checkState(open);
      out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      checkState(open);
      out.write(b);
    }

    @Override
    public void flush() throws IOException {
      checkState(open);
      out.flush();
    }

    @Override
    public void close() throws IOException {
      if (open) {
        try {
          out.closeEntry();
        } finally {
          open = false;
        }
      }
    }
  }
}

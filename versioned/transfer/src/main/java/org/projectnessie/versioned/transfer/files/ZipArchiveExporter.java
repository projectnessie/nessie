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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
  public long fixMaxFileSize(long userProvidedMaxFileSize) {
    return Math.max(64 * 1024, Math.min(10 * 1024 * 1024, userProvidedMaxFileSize));
  }

  @Override
  public void preValidate() {}

  @Override
  @Nonnull
  public Path getTargetPath() {
    return outputFile();
  }

  private final Set<DelayedOutputStream> activeOutputStreams = new HashSet<>();

  @Override
  @Nonnull
  public OutputStream newFileOutput(@Nonnull String fileName) throws IOException {
    checkArgument(
        fileName.indexOf('/') == -1 && fileName.indexOf('\\') == -1, "Directories not supported");
    checkArgument(!fileName.isEmpty(), "Invalid file name argument");

    var output = new DelayedOutputStream(fileName);
    synchronized (activeOutputStreams) {
      activeOutputStreams.add(output);
    }
    return output;
  }

  @Override
  public void close() throws Exception {
    try {
      List<DelayedOutputStream> activeOutputs;
      synchronized (activeOutputStreams) {
        activeOutputs = new ArrayList<>(activeOutputStreams);
      }
      for (var active : activeOutputs) {
        active.close();
      }

      zipOutput().close();
    } finally {
      deleteIfExists(outputFile());
      if (isRegularFile(tempOutputFile())) {
        move(tempOutputFile(), outputFile());
      }
    }
  }

  private void delayedFinished(DelayedOutputStream delayedOutputStream) {
    synchronized (activeOutputStreams) {
      activeOutputStreams.remove(delayedOutputStream);
    }
  }

  private final class DelayedOutputStream extends OutputStream {
    private boolean open = true;

    private final String name;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    private DelayedOutputStream(String name) {
      this.name = name;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      checkState(open);
      buffer.write(b, off, len);
    }

    @Override
    public void write(int b) {
      checkState(open);
      buffer.write(b);
    }

    @Override
    public void flush() throws IOException {
      checkState(open);
      buffer.flush();
    }

    @Override
    public void close() throws IOException {
      if (open) {
        try {
          ZipOutputStream out = zipOutput();
          out.putNextEntry(new ZipEntry(name));
          buffer.writeTo(out);
          out.closeEntry();
        } finally {
          delayedFinished(this);
          open = false;
        }
      }
    }
  }
}

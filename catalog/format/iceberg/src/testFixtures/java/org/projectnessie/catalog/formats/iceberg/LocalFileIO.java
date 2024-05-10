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
package org.projectnessie.catalog.formats.iceberg;

import static com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

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

  public static class SingleInputFileIO implements FileIO {
    private final AtomicBoolean check = new AtomicBoolean();
    private final InputFile inputFile;

    public SingleInputFileIO(InputFile inputFile) {
      this.inputFile = inputFile;
    }

    @Override
    public InputFile newInputFile(String path) {
      checkState(check.compareAndSet(false, true));
      return inputFile;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }
  }

  public static class NullPositionOutputStream extends WrappedPositionOutput {
    public NullPositionOutputStream() {
      super(nullOutputStream());
    }
  }

  public static class WrappedPositionOutput extends PositionOutputStream {
    private final OutputStream wrapped;
    private long pos;

    public WrappedPositionOutput(OutputStream wrapped) {
      this.wrapped = wrapped;
    }

    protected OutputStream wrapped() {
      return wrapped;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int b) throws IOException {
      wrapped.write(b);
      pos++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      wrapped.write(b, off, len);
      pos += len;
    }

    @Override
    public void write(byte[] b) throws IOException {
      wrapped.write(b);
      pos += b.length;
    }

    @Override
    public void flush() throws IOException {
      wrapped.flush();
    }

    @Override
    public void close() throws IOException {
      wrapped.close();
    }
  }

  public static class ByteArrayOutputFile implements OutputFile {
    private final String path;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final AtomicBoolean check = new AtomicBoolean();

    public ByteArrayOutputFile(String path) {
      this.path = path;
    }

    @Override
    public PositionOutputStream create() {
      checkState(check.compareAndSet(false, true));
      return new WrappedPositionOutput(output);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      checkState(check.compareAndSet(false, true));
      return new WrappedPositionOutput(output);
    }

    @Override
    public String location() {
      return path;
    }

    @Override
    public InputFile toInputFile() {
      throw new UnsupportedOperationException();
    }

    public byte[] toByteArray() {
      return output.toByteArray();
    }

    public int size() {
      return output.size();
    }
  }

  public static OutputFile nullOutputFile(String path) {
    return new OutputFile() {
      @Override
      public PositionOutputStream create() {
        return new NullPositionOutputStream();
      }

      @Override
      public PositionOutputStream createOrOverwrite() {
        return new NullPositionOutputStream();
      }

      @Override
      public String location() {
        return path;
      }

      @Override
      public InputFile toInputFile() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static Path asPath(String path) {
    URI uri = URI.create(path);
    if (uri.getScheme() == null) {
      return Paths.get(uri.getPath());
    }
    return Paths.get(uri);
  }
}

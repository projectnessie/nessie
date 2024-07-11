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
package org.projectnessie.gc.iceberg.mocks;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.ByteBufferInputStream;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;

public abstract class IcebergFileIOMocking implements FileIO {

  public static String tableBase(String tableUuid) {
    return String.format("mock://data/%s/", tableUuid);
  }

  public static String tableMetadataLocation(String tableUuid, long snapshotId) {
    return String.format("%s%d/foo.metadata.json", tableBase(tableUuid), snapshotId);
  }

  public static String viewMetadataLocation(String tableUuid, long snapshotId) {
    return String.format("%s%d/foo.metadata.json", tableBase(tableUuid), snapshotId);
  }

  public static String manifestListLocation(String tableUuid, long snapshotId) {
    return String.format("%s%d/manifest-list.json", tableBase(tableUuid), snapshotId);
  }

  public static String manifestFileLocation(String tableUuid, long snapshotId, int num) {
    return String.format("%s%d/manifest-file-%d.avro", tableBase(tableUuid), snapshotId, num);
  }

  public static String dataFileBase(String tableUuid, long snapshotId, int num) {
    return String.format("%s%d/data-file-%d", tableBase(tableUuid), snapshotId, num);
  }

  public static String dataFilePath(String tableUuid, long snapshotId, int num, int dataFileNum) {
    return String.format(
        "%s%d/data-file-%d-%d.parquet", tableBase(tableUuid), snapshotId, num, dataFileNum);
  }

  public static String dataFilePath(String dataFilePrefix, int dataFileNum) {
    return String.format("%s-%d.parquet", dataFilePrefix, dataFileNum);
  }

  public static IcebergFileIOMocking forSingleSnapshot(MockTableMetadata tableMetadata) {
    return new IcebergFileIOMocking() {
      @Override
      public InputFile newInputFile(String path) {
        String meta =
            tableMetadataLocation(tableMetadata.tableUuid(), tableMetadata.currentSnapshotId());
        if (path.equals(meta)) {
          return inputFile(meta, tableMetadata.jsonNode());
        }
        for (MockSnapshot snapshot : tableMetadata.snapshots()) {
          if (path.equals(snapshot.manifestListLocation())) {
            WrappedOutputFile output = new WrappedOutputFile(path);
            snapshot.generateManifestList(output);
            return output.toInputFile();
          }
          Optional<MockManifestFile> manifestFile =
              snapshot.manifestFiles().filter(mf -> mf.path().equals(path)).findFirst();
          if (manifestFile.isPresent()) {
            WrappedOutputFile output = new WrappedOutputFile(path);
            manifestFile.get().write(output);
            return output.toInputFile();
          }
        }
        return notFound(path);
      }
    };
  }

  public static IcebergFileIOMocking forSingleVersion(ViewMetadata viewMetadata) {
    return new IcebergFileIOMocking() {
      @Override
      public InputFile newInputFile(String path) {
        String meta = tableMetadataLocation(viewMetadata.uuid(), viewMetadata.currentVersionId());
        if (path.equals(meta)) {
          return inputFile(meta, ViewMetadataParser.toJson(viewMetadata).getBytes(UTF_8));
        }
        return notFound(path);
      }
    };
  }

  public static final class WrappedOutputFile implements OutputFile {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final String location;

    public WrappedOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      return new PositionOutputStream() {
        @Override
        public long getPos() {
          return output.size();
        }

        @Override
        public void write(int b) {
          output.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
          output.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
          output.write(b, off, len);
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return create();
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return inputFile(location, asBytes());
    }

    public byte[] asBytes() {
      return output.toByteArray();
    }
  }

  public static InputFile notFound(String path) {
    return new InputFile() {
      @Override
      public long getLength() {
        throw new NotFoundException("%s", path);
      }

      @Override
      public SeekableInputStream newStream() {
        throw new NotFoundException("%s", path);
      }

      @Override
      public String location() {
        return path;
      }

      @Override
      public boolean exists() {
        return false;
      }
    };
  }

  public static InputFile inputFile(String location, JsonNode jsonNode) {
    return inputFile(location, jsonNode.toString().getBytes(UTF_8));
  }

  public static InputFile inputFile(String location, byte[] data) {
    return new InputFile() {
      @Override
      public long getLength() {
        return data.length;
      }

      @Override
      public SeekableInputStream newStream() {
        return ByteBufferInputStream.wrap(ByteBuffer.wrap(data));
      }

      @Override
      public String location() {
        return location;
      }

      @Override
      public boolean exists() {
        return true;
      }
    };
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

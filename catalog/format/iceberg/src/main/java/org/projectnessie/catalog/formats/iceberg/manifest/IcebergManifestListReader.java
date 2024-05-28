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
package org.projectnessie.catalog.formats.iceberg.manifest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;

public abstract class IcebergManifestListReader
    implements AutoCloseable, Iterator<IcebergManifestFile> {
  public abstract IcebergSpec spec();

  public abstract long snapshotId();

  public abstract long parentSnapshotId();

  public abstract long sequenceNumber();

  private IcebergManifestListReader() {}

  public static IcebergManifestListReader openManifestListReader(SeekableInput input) {
    try {
      DataFileReader<IcebergManifestFile> reader = null;
      try {
        ListEntryReader datumReader = new ListEntryReader();

        reader = new DataFileReader<>(input, datumReader);

        int formatVersion = Integer.parseInt(reader.getMetaString("format-version"));
        long snapshotId = Long.parseLong(reader.getMetaString("snapshot-id"));
        String parentIdString = reader.getMetaString("parent-snapshot-id");
        long parentSnapshotId =
            // TODO Iceberg can literally write the string "null" into this Avro metadata property
            parentIdString != null && !("null".equals(parentIdString))
                ? Long.parseLong(parentIdString)
                : -1L;
        long sequenceNumber =
            formatVersion >= 2 ? Long.parseLong(reader.getMetaString("sequence-number")) : 0L;

        IcebergSpec spec = IcebergSpec.forVersion(formatVersion);
        datumReader.avroSchema = spec.avroBundle().schemaManifestFile();

        try {
          return new ManifestListReaderImplList(
              reader, spec, snapshotId, parentSnapshotId, sequenceNumber);
        } finally {
          reader = null;
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static final class ListEntryReader implements DatumReader<IcebergManifestFile> {
    private Schema readSchema;
    private AvroTyped<IcebergManifestFile> avroSchema;

    @Override
    public void setSchema(Schema schema) {
      this.readSchema = schema;
    }

    @Override
    public IcebergManifestFile read(IcebergManifestFile reuse, Decoder in) throws IOException {
      return avroSchema.read(in, readSchema);
    }
  }

  private static final class ManifestListReaderImplList extends IcebergManifestListReader {

    private final DataFileReader<IcebergManifestFile> reader;
    private final IcebergSpec spec;
    private final long snapshotId;
    private final long parentSnapshotId;
    private final long sequenceNumber;

    private ManifestListReaderImplList(
        DataFileReader<IcebergManifestFile> reader,
        IcebergSpec spec,
        long snapshotId,
        long parentSnapshotId,
        long sequenceNumber) {
      this.reader = reader;
      this.spec = spec;
      this.snapshotId = snapshotId;
      this.parentSnapshotId = parentSnapshotId;
      this.sequenceNumber = sequenceNumber;
    }

    @Override
    public IcebergSpec spec() {
      return spec;
    }

    @Override
    public long snapshotId() {
      return snapshotId;
    }

    @Override
    public long parentSnapshotId() {
      return parentSnapshotId;
    }

    @Override
    public long sequenceNumber() {
      return sequenceNumber;
    }

    @Override
    public boolean hasNext() {
      return reader.hasNext();
    }

    @Override
    public IcebergManifestFile next() {
      return reader.next();
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }
}

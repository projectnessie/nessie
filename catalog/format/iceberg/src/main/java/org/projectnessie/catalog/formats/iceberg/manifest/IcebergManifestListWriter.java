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

import static org.projectnessie.catalog.formats.iceberg.manifest.AvroSerializationContext.dataSerializationContext;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

public abstract class IcebergManifestListWriter implements AutoCloseable {

  private IcebergManifestListWriter() {}

  public abstract IcebergManifestListWriter append(IcebergManifestFile manifestFile);

  public static IcebergManifestListWriter openManifestListWriter(
      IcebergManifestListWriterSpec writerSpec, OutputStream output) {
    DataFileWriter<IcebergManifestFile> entryWriter =
        buildDataFileWriter(writerSpec.spec().avroBundle().schemaManifestFile());
    entryWriter.setMeta("format-version", Integer.toString(writerSpec.spec().version()));
    entryWriter.setMeta("snapshot-id", String.valueOf(writerSpec.snapshotId()));
    if (writerSpec.parentSnapshotId() != null) {
      entryWriter.setMeta("parent-snapshot-id", String.valueOf(writerSpec.parentSnapshotId()));
    }
    if (writerSpec.spec().version() >= 2) {
      entryWriter.setMeta("sequence-number", String.valueOf(writerSpec.sequenceNumber()));
    }
    // TODO add 'iceberg.schema', which is the Avro schema as an Iceberg schema

    dataSerializationContext(writerSpec.tableProperties()).applyToDataFileWriter(entryWriter);

    Schema entryWriteSchema = writerSpec.writerSchema();

    OutputContext outputContext = new OutputContext(output, writerSpec.closeOutput());

    try {
      entryWriter = entryWriter.create(entryWriteSchema, outputContext);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new ManifestListWriterImpl(entryWriter);
  }

  private static DataFileWriter<IcebergManifestFile> buildDataFileWriter(
      AvroTyped<IcebergManifestFile> avroManifestFile) {
    DatumWriter<IcebergManifestFile> datumWriter =
        new DatumWriter<IcebergManifestFile>() {
          private Schema writeSchema;

          @Override
          public void setSchema(Schema schema) {
            this.writeSchema = schema;
          }

          @Override
          public void write(IcebergManifestFile datum, Encoder out) throws IOException {
            avroManifestFile.write(out, datum, writeSchema);
          }
        };

    return new DataFileWriter<>(datumWriter);
  }

  private static final class ManifestListWriterImpl extends IcebergManifestListWriter {
    private final DataFileWriter<IcebergManifestFile> entryWriter;

    ManifestListWriterImpl(DataFileWriter<IcebergManifestFile> entryWriter) {
      this.entryWriter = entryWriter;
    }

    @Override
    public void close() throws Exception {
      entryWriter.close();
    }

    @Override
    public IcebergManifestListWriter append(IcebergManifestFile entry) {
      try {
        entryWriter.append(entry);

        return this;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}

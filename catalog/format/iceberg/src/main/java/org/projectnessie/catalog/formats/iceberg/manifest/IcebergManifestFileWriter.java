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
import static org.projectnessie.catalog.formats.iceberg.manifest.AvroSerializationContext.deleteSerializationContext;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

public abstract class IcebergManifestFileWriter implements AutoCloseable {

  private IcebergManifestFileWriter() {}

  public abstract IcebergManifestFileWriter append(
      IcebergDataFile dataFile,
      IcebergManifestEntryStatus status,
      Long fileSequenceNumber,
      Long sequenceNumber,
      Long snapshotId);

  public abstract IcebergManifestFileWriter append(IcebergManifestEntry entry);

  public abstract IcebergManifestFile finish() throws IOException;

  public static IcebergManifestFileWriter openManifestFileWriter(
      IcebergManifestFileWriterSpec writerSpec, OutputStream output) {
    DataFileWriter<IcebergManifestEntry> entryWriter = buildDataFileWriter(writerSpec);
    entryWriter.setMeta("schema", writerSpec.schemaAsJsonString());
    entryWriter.setMeta("partition-spec", writerSpec.partitionSpecAsJsonString());
    entryWriter.setMeta("partition-spec-id", Integer.toString(writerSpec.partitionSpec().specId()));
    entryWriter.setMeta("format-version", Integer.toString(writerSpec.spec().version()));
    // TODO add 'iceberg.schema', which is the Avro schema as an Iceberg schema
    if (writerSpec.spec().version() >= 2) {
      entryWriter.setMeta("content", writerSpec.content().stringValue());
    }

    AvroSerializationContext serializationContext;
    switch (writerSpec.content()) {
      case DATA:
        serializationContext = dataSerializationContext(writerSpec.tableProperties());
        break;
      case DELETES:
        serializationContext = deleteSerializationContext(writerSpec.tableProperties());
        break;
      default:
        throw new IllegalStateException("Unknown manifest content " + writerSpec.content());
    }
    serializationContext.applyToDataFileWriter(entryWriter);

    Schema entryWriteSchema = writerSpec.writerSchema();

    OutputContext outputContext = new OutputContext(output, writerSpec.closeOutput());

    try {
      entryWriter = entryWriter.create(entryWriteSchema, outputContext);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return new FileEntryWriterImpl(
        entryWriter,
        outputContext,
        new IcebergColumnStatsCollector(writerSpec.schema(), writerSpec.partitionSpec()),
        IcebergManifestFile.builder()
            .sequenceNumber(writerSpec.sequenceNumber())
            .minSequenceNumber(writerSpec.minSequenceNumber())
            .manifestPath(writerSpec.manifestPath())
            //
            .content(writerSpec.content())
            .addedSnapshotId(writerSpec.addedSnapshotId())
            .partitionSpecId(writerSpec.partitionSpec().specId())
            .keyMetadata(writerSpec.keyMetadata()));
  }

  private static DataFileWriter<IcebergManifestEntry> buildDataFileWriter(
      IcebergManifestFileWriterSpec writerSpec) {
    DatumWriter<IcebergManifestEntry> datumWriter =
        new DatumWriter<IcebergManifestEntry>() {
          private Schema writeSchema;
          private final AvroTyped<IcebergManifestEntry> avroManifestEntry =
              writerSpec.spec().avroBundle().schemaManifestEntry();

          @Override
          public void setSchema(Schema schema) {
            this.writeSchema = schema;
          }

          @Override
          public void write(IcebergManifestEntry datum, Encoder out) throws IOException {
            avroManifestEntry.write(out, datum, writeSchema);
          }
        };

    return new DataFileWriter<>(datumWriter);
  }

  private static final class FileEntryWriterImpl extends IcebergManifestFileWriter {
    private final DataFileWriter<IcebergManifestEntry> entryWriter;
    private final OutputContext outputContext;
    private final IcebergManifestFile.Builder manifestFile;
    private final IcebergColumnStatsCollector statsCollector;

    FileEntryWriterImpl(
        DataFileWriter<IcebergManifestEntry> entryWriter,
        OutputContext outputContext,
        IcebergColumnStatsCollector statsCollector,
        IcebergManifestFile.Builder manifestFile) {
      this.entryWriter = entryWriter;
      this.outputContext = outputContext;
      this.manifestFile = manifestFile;
      this.statsCollector = statsCollector;
    }

    @Override
    public void close() throws Exception {
      entryWriter.close();
    }

    @Override
    public IcebergManifestFileWriter append(
        IcebergDataFile dataFile,
        IcebergManifestEntryStatus status,
        Long fileSequenceNumber,
        Long sequenceNumber,
        Long snapshotId) {
      IcebergManifestEntry.Builder entry =
          IcebergManifestEntry.builder()
              .dataFile(dataFile)
              .status(status)
              .snapshotId(snapshotId)
              .fileSequenceNumber(fileSequenceNumber)
              .sequenceNumber(sequenceNumber);
      return append(entry.build());
    }

    @Override
    public IcebergManifestFileWriter append(IcebergManifestEntry entry) {
      try {
        entryWriter.append(entry);

        statsCollector.addManifestEntry(entry);

        return this;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public IcebergManifestFile finish() throws IOException {
      entryWriter.flush();

      statsCollector.addToManifestFileBuilder(manifestFile);

      return manifestFile.manifestLength(outputContext.bytesWritten()).build();
    }
  }
}

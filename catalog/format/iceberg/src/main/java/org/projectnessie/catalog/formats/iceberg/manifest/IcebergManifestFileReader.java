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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;

public abstract class IcebergManifestFileReader
    implements AutoCloseable, Iterator<IcebergManifestEntry> {
  public abstract IcebergSpec spec();

  public abstract IcebergSchema schema();

  public abstract IcebergPartitionSpec partitionSpec();

  public abstract IcebergManifestContent content();

  private IcebergManifestFileReader() {}

  public static IcebergManifestFileReader openManifestReader(SeekableInput input) {

    try {
      DataFileReader<IcebergManifestEntry> reader = null;
      try {
        FileEntryReader datumReader = new FileEntryReader();

        reader = new DataFileReader<>(input, datumReader);

        String schemaJson = reader.getMetaString("schema");
        String specJson = reader.getMetaString("partition-spec");
        int specId = Integer.parseInt(reader.getMetaString("partition-spec-id"));
        int formatVersion = Integer.parseInt(reader.getMetaString("format-version"));
        IcebergManifestContent content =
            formatVersion >= 2
                ? IcebergManifestContent.fromStringValue(reader.getMetaString("content"))
                : IcebergManifestContent.DATA;

        IcebergSpec spec = IcebergSpec.forVersion(formatVersion);
        datumReader.avroSchema = spec.avroBundle().schemaManifestEntry();
        IcebergSchema schema = spec.jsonReader().readValue(schemaJson, IcebergSchema.class);
        IcebergPartitionSpec partitionSpec;
        try (JsonParser parser = spec.jsonReader().createParser(specJson)) {
          try {
            List<IcebergPartitionField> partitionFields =
                parser.readValueAs(new TypeReference<List<IcebergPartitionField>>() {});
            partitionSpec = IcebergPartitionSpec.partitionSpec(specId, partitionFields);
          } catch (IOException e) {
            // workaround for https://github.com/apache/iceberg-python/pull/846
            partitionSpec = spec.jsonReader().readValue(specJson, IcebergPartitionSpec.class);
          }
        }

        try {
          return new ManifestFileEntryReaderImpl(reader, spec, schema, partitionSpec, content);
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

  private static final class FileEntryReader implements DatumReader<IcebergManifestEntry> {
    private Schema readSchema;
    private AvroTyped<IcebergManifestEntry> avroSchema;

    @Override
    public void setSchema(Schema schema) {
      this.readSchema = schema;
    }

    @Override
    public IcebergManifestEntry read(IcebergManifestEntry reuse, Decoder in) throws IOException {
      return avroSchema.read(in, readSchema);
    }
  }

  private static final class ManifestFileEntryReaderImpl extends IcebergManifestFileReader {

    private final DataFileReader<IcebergManifestEntry> reader;
    private final IcebergSpec spec;
    private final IcebergSchema schema;
    private final IcebergPartitionSpec partitionSpec;
    private final IcebergManifestContent content;

    private ManifestFileEntryReaderImpl(
        DataFileReader<IcebergManifestEntry> reader,
        IcebergSpec spec,
        IcebergSchema schema,
        IcebergPartitionSpec partitionSpec,
        IcebergManifestContent content) {
      this.reader = reader;
      this.spec = spec;
      this.schema = schema;
      this.partitionSpec = partitionSpec;
      this.content = content;
    }

    @Override
    public IcebergSpec spec() {
      return spec;
    }

    @Override
    public IcebergSchema schema() {
      return schema;
    }

    @Override
    public IcebergPartitionSpec partitionSpec() {
      return partitionSpec;
    }

    @Override
    public IcebergManifestContent content() {
      return content;
    }

    @Override
    public boolean hasNext() {
      return reader.hasNext();
    }

    @Override
    public IcebergManifestEntry next() {
      return reader.next();
    }

    @Override
    public void close() throws Exception {
      reader.close();
    }
  }
}

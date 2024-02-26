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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.dataFilePath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types.StructType;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockManifestFile implements IndexedRecord {

  @Nonnull
  public abstract String path();

  @Value.Default
  public int partitionSpecId() {
    return 0;
  }

  @Value.Default
  public ManifestContent content() {
    return ManifestContent.DATA;
  }

  @Value.Default
  @Nullable
  public ByteBuffer keyMetadata() {
    return null;
  }

  @Value.Default
  public long length() {
    return 42L;
  }

  @Value.Default
  public long sequenceNumber() {
    return 0;
  }

  @Value.Default
  public long minSequenceNumber() {
    return 0;
  }

  @Value.Default
  @Nullable
  public Long snapshotId() {
    return 0L;
  }

  @Value.Default
  @Nullable
  public Integer addedFilesCount() {
    return 0;
  }

  @Value.Default
  @Nullable
  public Long addedRowsCount() {
    return 0L;
  }

  @Value.Default
  @Nullable
  public Integer existingFilesCount() {
    return 0;
  }

  @Value.Default
  @Nullable
  public Long existingRowsCount() {
    return 0L;
  }

  @Value.Default
  @Nullable
  public Integer deletedFilesCount() {
    return 0;
  }

  @Value.Default
  @Nullable
  public Long deletedRowsCount() {
    return 0L;
  }

  @Value.Default
  public int numEntries() {
    return 1;
  }

  public abstract String baseDataFilePath();

  @Value.Auxiliary
  public List<MockManifestEntry> manifestEntries() {
    MockTableMetadata mockTableMeta = MockTableMetadata.empty();
    MockPartitionSpec mockPartitionSpec = mockTableMeta.partitionSpec(0);
    org.apache.iceberg.Schema icebergSchema = mockTableMeta.schema(0).toSchema();
    PartitionSpec partitionSpec = mockPartitionSpec.toPartitionSpec(icebergSchema);
    StructType partitionType = partitionSpec.partitionType();

    return IntStream.range(0, numEntries())
        .mapToObj(
            i ->
                ImmutableMockManifestEntry.builder()
                    .sequenceNumber(0L)
                    .snapshotId(0L)
                    .filePath(dataFilePath(baseDataFilePath(), i))
                    .partitionType(partitionType)
                    .build())
        .collect(Collectors.toList());
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public void write(OutputFile output) {
    MockTableMetadata mockTableMeta = MockTableMetadata.empty();
    MockPartitionSpec mockPartitionSpec = mockTableMeta.partitionSpec(0);
    org.apache.iceberg.Schema icebergSchema = mockTableMeta.schema(0).toSchema();
    PartitionSpec partitionSpec = mockPartitionSpec.toPartitionSpec(icebergSchema);
    StructType partitionType = partitionSpec.partitionType();

    try {
      String partitionSpecJson = PartitionSpecParser.toJson(partitionSpec);
      JsonNode partitionSpecFields =
          MAPPER.readValue(partitionSpecJson, JsonNode.class).get("fields");

      org.apache.iceberg.Schema manifestSchema = entrySchema(partitionType);
      try (FileAppender<Object> writer =
          Avro.write(output)
              .schema(manifestSchema)
              .named("manifest_entry")
              .meta("schema", SchemaParser.toJson(icebergSchema))
              .meta("partition-spec", partitionSpecFields.toString())
              .meta("partition-spec-id", String.valueOf(partitionSpecId()))
              .meta("format-version", "2")
              .meta("content", "deletes")
              .overwrite()
              .build()) {

        for (MockManifestEntry entry : manifestEntries()) {
          writer.add(entry);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static org.apache.iceberg.Schema entrySchema(StructType partitionType) {
    return wrapFileSchema(fileType(partitionType));
  }

  static org.apache.iceberg.Schema wrapFileSchema(StructType fileSchema) {
    // this is used to build projection schemas
    return new org.apache.iceberg.Schema(
        MockManifestEntry.STATUS,
        MockManifestEntry.SNAPSHOT_ID,
        MockManifestEntry.SEQUENCE_NUMBER,
        required(MockManifestEntry.DATA_FILE_ID, "data_file", fileSchema));
  }

  static StructType fileType(StructType partitionType) {
    return StructType.of(
        DataFile.CONTENT.asRequired(),
        DataFile.FILE_PATH,
        DataFile.FILE_FORMAT,
        required(
            DataFile.PARTITION_ID, DataFile.PARTITION_NAME, partitionType, DataFile.PARTITION_DOC),
        DataFile.RECORD_COUNT,
        DataFile.FILE_SIZE,
        DataFile.COLUMN_SIZES,
        DataFile.VALUE_COUNTS,
        DataFile.NULL_VALUE_COUNTS,
        DataFile.NAN_VALUE_COUNTS,
        DataFile.LOWER_BOUNDS,
        DataFile.UPPER_BOUNDS,
        DataFile.KEY_METADATA,
        DataFile.SPLIT_OFFSETS,
        DataFile.EQUALITY_IDS,
        DataFile.SORT_ORDER_ID);
  }

  private static final Schema AVRO_SCHEMA =
      AvroSchemaUtil.convert(ManifestFile.schema(), "manifest_file");

  @Override
  @Value.Auxiliary
  public Schema getSchema() {
    return AVRO_SCHEMA;
  }

  @Override
  public void put(int i, Object v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int i) {
    String field = AVRO_SCHEMA.getFields().get(i).name();
    switch (field) {
      case "manifest_path":
        return path();
      case "manifest_length":
        return length();
      case "partition_spec_id":
        return partitionSpecId();
      case "content":
        return content().ordinal();
      case "sequence_number":
        return sequenceNumber();
      case "min_sequence_number":
        return minSequenceNumber();
      case "added_snapshot_id":
        return snapshotId();
      case "added_files_count":
        return addedFilesCount();
      case "existing_files_count":
        return existingFilesCount();
      case "deleted_files_count":
        return deletedFilesCount();
      case "added_rows_count":
        return addedRowsCount();
      case "existing_rows_count":
        return existingRowsCount();
      case "deleted_rows_count":
        return deletedRowsCount();
      case "partitions":
        return null; // TODO ?
      case "key_metadata":
        return null; // TODO ?
      default:
        throw new IllegalArgumentException("Unknown field '" + field + "'");
    }
  }
}

/*
 * Copyright (C) 2024 Dremio
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface IcebergManifestFileWriterSpec {

  static Builder builder() {
    return ImmutableIcebergManifestFileWriterSpec.builder();
  }

  IcebergSpec spec();

  IcebergSchema schema();

  @Value.Default
  default String schemaAsJsonString() {
    try {
      return spec().jsonWriter().writeValueAsString(schema());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  IcebergPartitionSpec partitionSpec();

  @Value.Default
  default String partitionSpecAsJsonString() {
    try {
      return spec().jsonWriter().writeValueAsString(partitionSpec().fields());
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  long addedSnapshotId();

  IcebergManifestContent content();

  long sequenceNumber();

  long minSequenceNumber();

  Map<String, String> tableProperties();

  @Nullable
  @jakarta.annotation.Nullable
  byte[] keyMetadata();

  String manifestPath();

  @Value.Default
  default Schema writerSchema() {
    AvroTyped<IcebergManifestEntry> avroManifestEntry = spec().avroBundle().schemaManifestEntry();
    return avroManifestEntry.writeSchema(
        AvroReadWriteContext.builder()
            .putSchemaOverride("data_file.partition", partitionSpec().avroSchema(schema(), "r102"))
            .build());
  }

  @Value.Default
  default boolean closeOutput() {
    return false;
  }

  @SuppressWarnings("unused")
  interface Builder {

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergManifestFileWriterSpec writerSpec);

    @CanIgnoreReturnValue
    Builder spec(IcebergSpec spec);

    @CanIgnoreReturnValue
    Builder schema(IcebergSchema schema);

    // Intentionally omitting setter for schemaAsJsonString, because it is derived from
    // partitionSpec and only present to be able to reuse the generated value in derived
    // IcebergManifestFileWriterSpec instances.

    @CanIgnoreReturnValue
    Builder partitionSpec(IcebergPartitionSpec partitionSpec);

    // Intentionally omitting setter for partitionSpecAsJsonString, because it is derived from
    // partitionSpec and only present to be able to reuse the generated value in derived
    // IcebergManifestFileWriterSpec instances.

    @CanIgnoreReturnValue
    Builder addedSnapshotId(long addedSnapshotId);

    @CanIgnoreReturnValue
    Builder content(IcebergManifestContent content);

    @CanIgnoreReturnValue
    Builder sequenceNumber(long sequenceNumber);

    @CanIgnoreReturnValue
    Builder minSequenceNumber(long minSequenceNumber);

    @CanIgnoreReturnValue
    Builder keyMetadata(byte[] keyMetadata);

    @CanIgnoreReturnValue
    Builder closeOutput(boolean closeOutput);

    @CanIgnoreReturnValue
    Builder manifestPath(String manifestPath);

    @CanIgnoreReturnValue
    Builder putTableProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putTableProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder tableProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllTableProperties(Map<String, ? extends String> ent0ries);

    IcebergManifestFileWriterSpec build();
  }
}

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
public interface IcebergManifestListWriterSpec {

  static Builder builder() {
    return ImmutableIcebergManifestListWriterSpec.builder();
  }

  IcebergSpec spec();

  IcebergSchema schema();

  IcebergPartitionSpec partitionSpec();

  Map<String, String> tableProperties();

  long snapshotId();

  @Nullable
  @jakarta.annotation.Nullable
  Long parentSnapshotId();

  @Value.Default
  default long sequenceNumber() {
    return 0L;
  }

  @Value.Default
  default boolean closeOutput() {
    return false;
  }

  @Value.Default
  default Schema writerSchema() {
    AvroTyped<IcebergManifestFile> avroManifestEntry = spec().avroBundle().schemaManifestFile();

    AvroReadWriteContext avroReadWriteContext = null;
    // AvroReadWriteContext avroReadWriteContext = AvroReadWriteContext.builder()
    //  .putSchemaOverride("data_file.partition", partitionSpec().avroSchema(schema(), "r102"))
    //  .build();

    return avroManifestEntry.writeSchema(avroReadWriteContext);
  }

  interface Builder {

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergManifestListWriterSpec writerSpec);

    @CanIgnoreReturnValue
    Builder spec(IcebergSpec spec);

    @CanIgnoreReturnValue
    Builder schema(IcebergSchema schema);

    @CanIgnoreReturnValue
    Builder partitionSpec(IcebergPartitionSpec partitionSpec);

    @CanIgnoreReturnValue
    Builder snapshotId(long snapshotId);

    @CanIgnoreReturnValue
    Builder parentSnapshotId(Long parentSnapshotId);

    @CanIgnoreReturnValue
    Builder sequenceNumber(long sequenceNumber);

    @CanIgnoreReturnValue
    Builder closeOutput(boolean closeOutput);

    @CanIgnoreReturnValue
    Builder putTableProperty(String key, String value);

    @CanIgnoreReturnValue
    Builder putTableProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder tableProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllTableProperties(Map<String, ? extends String> entries);

    IcebergManifestListWriterSpec build();
  }
}

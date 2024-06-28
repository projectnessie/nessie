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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import jakarta.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.BridgeToIceberg;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockManifestEntry implements IndexedRecord {

  public enum Status {
    EXISTING,
    ADDED,
    DELETED
  }

  @Value.Default
  public Status status() {
    return Status.ADDED;
  }

  @Nullable
  public abstract Long snapshotId();

  @Nullable
  public abstract Long sequenceNumber();

  public abstract String filePath();

  public abstract StructType partitionType();

  // ids for data-file columns are assigned from 1000
  static final Types.NestedField STATUS = required(0, "status", Types.IntegerType.get());
  static final Types.NestedField SNAPSHOT_ID = optional(1, "snapshot_id", Types.LongType.get());
  static final Types.NestedField SEQUENCE_NUMBER =
      optional(3, "sequence_number", Types.LongType.get());
  static final int DATA_FILE_ID = 2;

  @Override
  @Value.Auxiliary
  public Schema getSchema() {
    StructType fileSchema = MockManifestFile.fileType(partitionType());
    return AvroSchemaUtil.convert(
        new org.apache.iceberg.Schema(
            STATUS, SNAPSHOT_ID, SEQUENCE_NUMBER, required(DATA_FILE_ID, "data_file", fileSchema)),
        "manifest_entry");
  }

  @Override
  public void put(int i, Object v) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(int i) {
    String field = getSchema().getFields().get(i).name();
    switch (field) {
      case "status":
        return status().ordinal();
      case "snapshot_id":
        return snapshotId();
      case "sequence_number":
        return sequenceNumber();
      case "data_file":
        return BridgeToIceberg.dummyIndexedDataFile(filePath(), partitionType());
      default:
        throw new IllegalArgumentException("Unknown field '" + field + "'");
    }
  }
}

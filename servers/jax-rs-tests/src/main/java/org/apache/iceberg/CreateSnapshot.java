/*
 * Copyright (C) 2020 Dremio
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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.UUID;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;

public final class CreateSnapshot {
  private CreateSnapshot() {}

  public static TableMetadata createMetadata() {
    Snapshot snapshot =
        new BaseSnapshot(
            null,
            0,
            123L,
            -1L,
            Instant.now().getEpochSecond() * 1000,
            "test op",
            ImmutableMap.of("prop1", "val1"),
            0,
            "manifest list");
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get())).fields());
    return new TableMetadata(
        inputFile(),
        1,
        UUID.randomUUID().toString(),
        "path/to/data",
        0,
        System.currentTimeMillis(),
        1,
        schema.schemaId(),
        ImmutableList.of(schema),
        PartitionSpec.unpartitioned().specId(),
        ImmutableList.of(PartitionSpec.unpartitioned()),
        PartitionSpec.unpartitioned().lastAssignedFieldId(),
        SortOrder.unsorted().orderId(),
        ImmutableList.of(SortOrder.unsorted()),
        ImmutableMap.of("prop1", "val1", "prop2", "val2"),
        123L,
        ImmutableList.of(snapshot),
        ImmutableList.of(),
        ImmutableList.of());
  }

  private static InputFile inputFile() {
    return new InputFile() {
      @Override
      public long getLength() {
        return 0;
      }

      @Override
      public SeekableInputStream newStream() {
        return null;
      }

      @Override
      public String location() {
        return "/path/to/metadata.json";
      }

      @Override
      public boolean exists() {
        return false;
      }
    };
  }
}

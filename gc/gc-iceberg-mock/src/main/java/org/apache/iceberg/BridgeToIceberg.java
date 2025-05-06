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
package org.apache.iceberg;

import java.util.Collections;
import org.apache.iceberg.V2Metadata.DataFileWrapper;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types.StructType;

/** Uses package-private classes and functions from Iceberg, only for testing purposes. */
public final class BridgeToIceberg {

  private BridgeToIceberg() {}

  public static final Metrics DUMMY_METRICS =
      new Metrics(
          42L,
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap(),
          Collections.emptyMap());

  public static DataFile dummyDataFile(String filePath, StructType partitionType) {
    PartitionData partitionData = new PartitionData(partitionType);
    return new GenericDataFile(
        0, filePath, FileFormat.PARQUET, partitionData, 42L, DUMMY_METRICS, null, null, 0, null);
  }

  public static StructLike dummyIndexedDataFile(String filePath, StructType partitionType) {
    DataFile dataFile = dummyDataFile(filePath, partitionType);
    DataFileWrapper<?> indexed = new DataFileWrapper<>();
    indexed.wrap(dataFile);
    var avroSchema = AvroSchemaUtil.convert(partitionType);
    return new IndexedStructLike(avroSchema).wrap(indexed);
  }
}

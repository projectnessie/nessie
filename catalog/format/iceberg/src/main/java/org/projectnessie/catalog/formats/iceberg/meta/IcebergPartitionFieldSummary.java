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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface IcebergPartitionFieldSummary {
  static Builder builder() {
    return ImmutableIcebergPartitionFieldSummary.builder();
  }

  static IcebergPartitionFieldSummary icebergPartitionFieldSummary(
      boolean containsNull, byte[] lowerBound, byte[] upperBound, Boolean containsNan) {
    return ImmutableIcebergPartitionFieldSummary.of(
        containsNull, lowerBound, upperBound, containsNan);
  }

  boolean containsNull();

  // TODO subject to write.metadata.metrics.default / write.metadata.metrics.column.* settings !!
  //  Default is 'truncate(16)', see
  //  https://iceberg.apache.org/docs/1.3.0/configuration/#write-properties
  // TODO how is this encoded?? in IJ's Avro/Parquet viewer string values appear "interesting"
  // TODO Use Agrona-Collections
  @Nullable
  @jakarta.annotation.Nullable
  byte[] lowerBound();

  // TODO subject to write.metadata.metrics.default / write.metadata.metrics.column.* settings !!
  //  Default is 'truncate(16)', see
  //  https://iceberg.apache.org/docs/1.3.0/configuration/#write-properties
  // TODO how is this encoded?? in IJ's Avro/Parquet viewer string values appear "interesting"
  // TODO Iceberg does not add some suffix like Delta does ("tie breaker") for the max value, but
  //  uses Parquet's functionality, see
  //  org.apache.parquet.internal.column.columnindex.BinaryTruncator.truncateMax?
  // TODO Use Agrona-Collections
  @Nullable
  @jakarta.annotation.Nullable
  byte[] upperBound();

  @Nullable
  @jakarta.annotation.Nullable
  Boolean containsNan();

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder containsNull(boolean containsNull);

    @CanIgnoreReturnValue
    Builder lowerBound(byte[] lowerBound);

    @CanIgnoreReturnValue
    Builder upperBound(byte[] upperBound);

    @CanIgnoreReturnValue
    Builder containsNan(Boolean containsNan);

    IcebergPartitionFieldSummary build();
  }
}

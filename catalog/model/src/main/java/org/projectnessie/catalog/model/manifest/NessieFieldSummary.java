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
package org.projectnessie.catalog.model.manifest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.id.Hashable;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Summary about a field/column, used for both per data/delete file entries and for manifest-file
 * entries.
 */
@NessieImmutable
@JsonSerialize(as = ImmutableNessieFieldSummary.class)
@JsonDeserialize(as = ImmutableNessieFieldSummary.class)
public interface NessieFieldSummary extends Hashable {
  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher
        .hash(fieldId())
        .hash(columnSize())
        .hash(containsNull())
        .hash(containsNan())
        .hash(nullValueCount())
        .hash(nanValueCount())
        .hash(valueCount())
        .hash(lowerBound())
        .hash(upperBound());
  }

  // Only for Iceberg
  // TODO store the NessieId or both?
  Integer fieldId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Present for manifest file entries
  // Only for Iceberg
  Long columnSize();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Present for manifest file entries
  Boolean containsNull();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Present for manifest file entries
  // Only for Iceberg
  Boolean containsNan();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Present for data file entries
  Long nullValueCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Present for data file entries
  // Only for Iceberg
  Long nanValueCount();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  // Present for data file entries
  Long valueCount();

  // TODO In Iceberg, the partition-field statistics in a manifest-list entry are subject to
  //  write.metadata.metrics.default / write.metadata.metrics.column.* settings !!
  //  Default is 'truncate(16)', see
  //  https://iceberg.apache.org/docs/1.3.0/configuration/#write-properties
  // TODO how is this encoded?? in IJ's Avro/Parquet viewer string values appear "interesting"
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  byte[] lowerBound();

  // TODO In Iceberg, the partition-field statistics in a manifest-list entry are subject to
  //  write.metadata.metrics.default / write.metadata.metrics.column.* settings !!
  //  Default is 'truncate(16)', see
  //  https://iceberg.apache.org/docs/1.3.0/configuration/#write-properties
  // TODO how is this encoded?? in IJ's Avro/Parquet viewer string values appear "interesting"
  // TODO Iceberg does not add some suffix like Delta does ("tie breaker") for the max value, but
  //  uses Parquet's functionality, see
  //  org.apache.parquet.internal.column.columnindex.BinaryTruncator.truncateMax?
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  byte[] upperBound();

  static Builder builder() {
    return ImmutableNessieFieldSummary.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieFieldSummary instance);

    @CanIgnoreReturnValue
    Builder fieldId(Integer fieldId);

    @CanIgnoreReturnValue
    Builder columnSize(@Nullable Long columnSize);

    @CanIgnoreReturnValue
    Builder containsNull(Boolean containsNull);

    @CanIgnoreReturnValue
    Builder containsNan(Boolean containsNan);

    @CanIgnoreReturnValue
    Builder nullValueCount(@Nullable Long nullValueCount);

    @CanIgnoreReturnValue
    Builder nanValueCount(@Nullable Long nanValueCount);

    @CanIgnoreReturnValue
    Builder valueCount(@Nullable Long valueCount);

    @CanIgnoreReturnValue
    Builder lowerBound(byte[] lowerBound);

    @CanIgnoreReturnValue
    Builder upperBound(byte[] upperBound);

    NessieFieldSummary build();
  }
}

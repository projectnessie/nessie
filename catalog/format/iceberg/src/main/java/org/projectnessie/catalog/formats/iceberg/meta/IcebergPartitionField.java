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

import static com.google.common.base.Preconditions.checkState;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.MIN_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.INITIAL_COLUMN_ID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergPartitionField.class)
@JsonDeserialize(as = ImmutableIcebergPartitionField.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergPartitionField {

  static Builder builder() {
    return ImmutableIcebergPartitionField.builder();
  }

  static IcebergPartitionField partitionField(
      String name, String transform, int sourceId, int fieldId) {
    return ImmutableIcebergPartitionField.of(name, transform, sourceId, fieldId);
  }

  String name();

  @Value.Lazy
  @JsonIgnore
  default IcebergTransform transformInstance() {
    return IcebergTransform.fromString(transform());
  }

  default IcebergType type(IcebergSchema schema) {
    return schema.fields().stream()
        .filter(f -> f.id() == sourceId())
        .findFirst()
        .map(f -> transformInstance().transformedType(f.type()))
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Partition field source ID "
                        + sourceId()
                        + " is not contained in the given schema"));
  }

  String transform();

  /** Refers to a {@link IcebergNestedField#id()}. */
  int sourceId();

  /**
   * Partition-field-IDs start at {@code 1000} and can technically "collide" with {@link
   * IcebergNestedField#id() column/nested-field} {@link #sourceId() IDs}.
   */
  int fieldId();

  @Value.Check
  default void check() {
    checkState(
        fieldId() >= MIN_PARTITION_ID,
        "Partition field IDs must be greater than or equal to %s",
        MIN_PARTITION_ID);
    checkState(
        sourceId() >= INITIAL_COLUMN_ID,
        "Partition source IDs must be greater than or equal to %s",
        INITIAL_COLUMN_ID);
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergPartitionField field);

    @CanIgnoreReturnValue
    Builder name(String name);

    @CanIgnoreReturnValue
    Builder transform(String transform);

    @CanIgnoreReturnValue
    Builder sourceId(int sourceId);

    @CanIgnoreReturnValue
    Builder fieldId(int fieldId);

    @CanIgnoreReturnValue
    Builder clear();

    IcebergPartitionField build();
  }
}

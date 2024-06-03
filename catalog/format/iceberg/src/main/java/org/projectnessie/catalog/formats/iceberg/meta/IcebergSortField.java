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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Objects;
import org.projectnessie.catalog.model.schema.NessieNullOrder;
import org.projectnessie.catalog.model.schema.NessieSortDirection;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergSortField.class)
@JsonDeserialize(as = ImmutableIcebergSortField.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergSortField {

  static Builder builder() {
    return ImmutableIcebergSortField.builder();
  }

  static IcebergSortField sortField(
      String transform, int sourceId, NessieSortDirection direction, NessieNullOrder nullOrder) {
    return ImmutableIcebergSortField.of(transform, sourceId, direction, nullOrder);
  }

  String transform();

  // TODO need a way to retrieve the result-type of `transform()`, see
  //  org.apache.iceberg.transforms.Transforms.fromString(java.lang.String)
  //  see also IcebergPartitionField.transform

  int sourceId();

  NessieSortDirection direction();

  NessieNullOrder nullOrder();

  default boolean satisfies(IcebergSortField other) {
    if (Objects.equals(this, other)) {
      return true;
    } else if (sourceId() != other.sourceId()
        || !direction().equals(other.direction())
        || !nullOrder().equals(other.nullOrder())) {
      return false;
    }

    // TODO add an `IcebergTransform` type and implement checks for the following
    //  return transform().satisfiesOrderOf(other.transform());
    return transform().equals(other.transform());
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergSortField field);

    @CanIgnoreReturnValue
    Builder transform(String transform);

    @CanIgnoreReturnValue
    Builder sourceId(int sourceId);

    @CanIgnoreReturnValue
    Builder direction(NessieSortDirection direction);

    @CanIgnoreReturnValue
    Builder nullOrder(NessieNullOrder nullOrder);

    IcebergSortField build();
  }
}

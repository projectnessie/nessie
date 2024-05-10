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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergSortOrder.class)
@JsonDeserialize(as = ImmutableIcebergSortOrder.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergSortOrder {

  int INITIAL_SORT_ORDER_ID = 1;

  static Builder builder() {
    return ImmutableIcebergSortOrder.builder();
  }

  static IcebergSortOrder sortOrder(int orderId, List<IcebergSortField> fields) {
    return ImmutableIcebergSortOrder.of(orderId, fields);
  }

  IcebergSortOrder UNSORTED_ORDER = sortOrder(0, Collections.emptyList());

  static IcebergSortOrder unsorted() {
    return UNSORTED_ORDER;
  }

  int orderId();

  IcebergSortOrder withOrderId(int orderId);

  List<IcebergSortField> fields();

  @JsonIgnore
  default boolean isUnsorted() {
    return fields().isEmpty();
  }

  default boolean sameOrder(IcebergSortOrder anotherSortOrder) {
    return fields().equals(anotherSortOrder.fields());
  }

  default boolean satisfies(IcebergSortOrder anotherSortOrder) {
    // any ordering satisfies an unsorted ordering
    if (anotherSortOrder.isUnsorted()) {
      return true;
    }

    // this ordering cannot satisfy an ordering with more sort fields
    if (anotherSortOrder.fields().size() > fields().size()) {
      return false;
    }

    // this ordering has either more or the same number of sort fields
    return IntStream.range(0, anotherSortOrder.fields().size())
        .allMatch(index -> fields().get(index).satisfies(anotherSortOrder.fields().get(index)));
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergSortOrder sortOrder);

    @CanIgnoreReturnValue
    Builder orderId(int orderId);

    @CanIgnoreReturnValue
    Builder addField(IcebergSortField element);

    @CanIgnoreReturnValue
    Builder addFields(IcebergSortField... elements);

    @CanIgnoreReturnValue
    Builder fields(Iterable<? extends IcebergSortField> elements);

    @CanIgnoreReturnValue
    Builder addAllFields(Iterable<? extends IcebergSortField> elements);

    IcebergSortOrder build();
  }
}

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
package org.projectnessie.catalog.model.schema;

import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieSortDefinition.class)
@JsonDeserialize(as = ImmutableNessieSortDefinition.class)
public interface NessieSortDefinition {

  int NO_SORT_ORDER_ID = -1;

  NessieId id();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieSortField> columns();

  @Value.Default
  default int icebergSortOrderId() {
    return NO_SORT_ORDER_ID;
  }

  static NessieSortDefinition nessieSortDefinition(
      List<NessieSortField> fields, int icebergSortOrderId) {
    return nessieSortDefinition(
        nessieIdHasher("NessieSortDefinition")
            .hashCollection(fields)
            .hash(icebergSortOrderId)
            .generate(),
        fields,
        icebergSortOrderId);
  }

  static NessieSortDefinition nessieSortDefinition(
      NessieId sortDefinitionId, List<NessieSortField> fields, int icebergSortOrderId) {
    return ImmutableNessieSortDefinition.of(sortDefinitionId, fields, icebergSortOrderId);
  }
}

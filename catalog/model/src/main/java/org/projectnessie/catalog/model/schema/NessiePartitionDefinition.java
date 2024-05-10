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

import static java.util.function.Function.identity;
import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;
import static org.projectnessie.catalog.model.schema.NessiePartitionField.NO_FIELD_ID;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessiePartitionDefinition.class)
@JsonDeserialize(as = ImmutableNessiePartitionDefinition.class)
public interface NessiePartitionDefinition {

  int NO_PARTITION_SPEC_ID = -1;

  NessieId id();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessiePartitionField> fields();

  @Value.Default
  default int icebergId() {
    return NO_PARTITION_SPEC_ID;
  }

  @Value.Lazy
  default Map<Integer, NessiePartitionField> icebergColumnMap() {
    return fields().stream()
        .filter(f -> f.icebergId() != NO_FIELD_ID)
        .collect(Collectors.toMap(NessiePartitionField::icebergId, identity()));
  }

  static NessiePartitionDefinition nessiePartitionDefinition(
      List<NessiePartitionField> fields, int icebergSpecId) {
    return nessiePartitionDefinition(
        nessieIdHasher("NessiePartitionDefinition")
            .hashCollection(fields)
            .hash(icebergSpecId)
            .generate(),
        fields,
        icebergSpecId);
  }

  static NessiePartitionDefinition nessiePartitionDefinition(
      NessieId partitionDefinitionId, List<NessiePartitionField> fields, int icebergSpecId) {
    return ImmutableNessiePartitionDefinition.of(partitionDefinitionId, fields, icebergSpecId);
  }
}

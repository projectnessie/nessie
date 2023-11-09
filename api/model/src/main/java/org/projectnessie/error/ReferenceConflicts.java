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
package org.projectnessie.error;

import static java.util.Collections.singletonList;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.model.Conflict;

@Value.Immutable
@JsonSerialize(as = ImmutableReferenceConflicts.class)
@JsonDeserialize(as = ImmutableReferenceConflicts.class)
@JsonTypeName(ReferenceConflicts.TYPE)
public interface ReferenceConflicts extends NessieErrorDetails {
  String TYPE = "REFERENCE_CONFLICTS";

  @Override
  default String getType() {
    return TYPE;
  }

  @JsonInclude(Include.NON_EMPTY)
  @Value.Parameter(order = 1)
  List<Conflict> conflicts();

  static ReferenceConflicts referenceConflicts(Conflict singleConflict) {
    return referenceConflicts(singletonList(singleConflict));
  }

  static ReferenceConflicts referenceConflicts(Iterable<? extends Conflict> conflicts) {
    return ImmutableReferenceConflicts.of(conflicts);
  }
}

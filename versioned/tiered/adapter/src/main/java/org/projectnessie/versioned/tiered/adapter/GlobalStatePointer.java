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
package org.projectnessie.versioned.tiered.adapter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceNotFoundException;

@Value.Immutable(lazyhash = true)
@JsonSerialize(as = ImmutableGlobalStatePointer.class)
@JsonDeserialize(as = ImmutableGlobalStatePointer.class)
public interface GlobalStatePointer {
  Hash getGlobalId();

  // TODO think about how to deal with many named references - for example a ton of tags.
  //  Does managing this map with an LRU policy and moving old (or stale) out to
  //   a separate entity/row work? --> "hot" refs stay in the global-pointer row,
  //   "cold" refs are moved out.
  //  Shall tags be managed in a separate map and database entity?
  Map<NamedRef, Hash> getNamedReferences();

  static GlobalStatePointer of(Hash globalId, Map<NamedRef, Hash> namedReferences) {
    return ImmutableGlobalStatePointer.builder()
        .globalId(globalId)
        .namedReferences(namedReferences)
        .build();
  }

  default Hash branchHead(NamedRef ref) throws ReferenceNotFoundException {
    Hash branchHead = getNamedReferences().get(ref);
    if (branchHead == null) {
      throw new ReferenceNotFoundException(
          String.format("Named reference '%s' not found", ref.getName()));
    }
    return branchHead;
  }
}

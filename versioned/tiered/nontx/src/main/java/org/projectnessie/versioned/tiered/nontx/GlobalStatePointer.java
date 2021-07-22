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
package org.projectnessie.versioned.tiered.nontx;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;

/** Points to the most recent {@link GlobalStateLogEntry} and HEADs of all named references. */
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
  /**
   * List containing the named-reference-to-hash mappings.
   *
   * <p>A list is more efficient to (de)serialize. Since the modifications to this list are not
   * complex and (de)serialization has to visit each element anyway, a map would be way more
   * expensive.
   */
  List<Ref> getNamedReferences();

  static GlobalStatePointer of(Hash globalId, List<Ref> namedReferences) {
    return ImmutableGlobalStatePointer.builder()
        .globalId(globalId)
        .namedReferences(namedReferences)
        .build();
  }

  default GlobalStatePointer update(Hash newGlobalHead, NamedRef ref, Hash toHead) {
    List<Ref> curr = getNamedReferences();
    List<Ref> newRefs = new ArrayList<>(curr.size() + 1);

    if (toHead == null) {
      for (Ref r : curr) {
        if (!r.getRef().equals(ref)) {
          newRefs.add(r);
        }
      }
    } else {
      boolean done = false;
      for (Ref r : curr) {
        if (r.getRef().equals(ref)) {
          newRefs.add(Ref.of(toHead, ref));
          done = true;
        } else {
          newRefs.add(r);
        }
      }
      if (!done) {
        newRefs.add(Ref.of(toHead, ref));
      }
    }

    return GlobalStatePointer.of(newGlobalHead, newRefs);
  }

  default Hash hashForReference(NamedRef ref) {
    for (Ref r : getNamedReferences()) {
      if (r.getRef().equals(ref)) {
        return r.getHash();
      }
    }
    return null;
  }
}

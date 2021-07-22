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
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.tiered.adapter.KeyWithBytes;

/** Represents a global-state-log-entry that tracks the global state of all known contents. */
@Value.Immutable(lazyhash = true)
@JsonSerialize(as = ImmutableGlobalStateLogEntry.class)
@JsonDeserialize(as = ImmutableGlobalStateLogEntry.class)
public interface GlobalStateLogEntry {
  /** Creation timestamp in microseconds since epoch. */
  long getCreatedTime();

  Hash getId();

  List<Hash> getParents();

  /**
   * List containing the global-content-keys along with content-type and global-content.
   *
   * <p>A list is more efficient to (de)serialize. Since the modifications to this list are not
   * complex and (de)serialization has to visit each element anyway, a map would be way more
   * expensive.
   */
  List<KeyWithBytes> getPuts();

  static GlobalStateLogEntry of(
      long createdTime, Hash id, List<Hash> parents, List<KeyWithBytes> puts) {
    return ImmutableGlobalStateLogEntry.builder()
        .createdTime(createdTime)
        .id(id)
        .parents(parents)
        .puts(puts)
        .build();
  }
}

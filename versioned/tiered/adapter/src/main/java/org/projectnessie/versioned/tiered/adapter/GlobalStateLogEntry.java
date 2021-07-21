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
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;

@Value.Immutable
@JsonSerialize(as = ImmutableGlobalStateLogEntry.class)
@JsonDeserialize(as = ImmutableGlobalStateLogEntry.class)
public interface GlobalStateLogEntry {
  long getCreatedTime();

  Hash getId();

  List<Hash> getParents();

  Map<Key, ByteString> getStatePuts();

  static GlobalStateLogEntry of(
      long createdTime, Hash id, List<Hash> parents, Map<Key, ByteString> statePuts) {
    return ImmutableGlobalStateLogEntry.builder()
        .createdTime(createdTime)
        .id(id)
        .parents(parents)
        .statePuts(statePuts)
        .build();
  }
}

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
import org.immutables.value.Value;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;

@Value.Immutable(lazyhash = true)
@JsonSerialize(as = ImmutableRef.class)
@JsonDeserialize(as = ImmutableRef.class)
public interface Ref {
  NamedRef getRef();

  Hash getHash();

  static Ref of(Hash hash, NamedRef ref) {
    return ImmutableRef.builder().ref(ref).hash(hash).build();
  }
}

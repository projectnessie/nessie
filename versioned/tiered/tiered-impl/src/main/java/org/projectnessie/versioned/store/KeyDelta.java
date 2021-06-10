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
package org.projectnessie.versioned.store;

import javax.annotation.Nullable;
import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithPayload;

/** Key-deltas of an L3. */
@Immutable
public interface KeyDelta {
  Key getKey();

  Id getId();

  @Nullable
  Byte getPayload();

  static KeyDelta of(Key key, Id id, Byte payload) {
    return ImmutableKeyDelta.builder().key(key).id(id).payload(payload).build();
  }

  static KeyDelta of(WithPayload<Key> key, Id id) {
    return ImmutableKeyDelta.builder().key(key.getValue()).id(id).payload(key.getPayload()).build();
  }

  default WithPayload<Key> toKeyWithPayload() {
    return WithPayload.of(getPayload(), getKey());
  }
}

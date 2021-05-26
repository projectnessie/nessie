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
package org.projectnessie.versioned.impl;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.WithPayload;

@Value.Immutable
public interface InternalKeyWithPayload {

  InternalKey getKey();

  @Nullable
  Byte getPayload();

  default WithPayload<Key> toKey() {
    return WithPayload.of(getPayload(), getKey().toKey());
  }

  static InternalKeyWithPayload of(Byte payload, Key key) {
    return ImmutableInternalKeyWithPayload.builder()
        .payload(payload)
        .key(new InternalKey(key))
        .build();
  }

  static InternalKeyWithPayload of(Byte payload, InternalKey key) {
    return ImmutableInternalKeyWithPayload.builder().payload(payload).key(key).build();
  }
}

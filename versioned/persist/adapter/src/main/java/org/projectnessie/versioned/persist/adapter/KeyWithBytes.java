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
package org.projectnessie.versioned.persist.adapter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.protobuf.ByteString;
import org.immutables.value.Value;
import org.projectnessie.versioned.Key;

/** Composite of key, contents-type and contents. */
@Value.Immutable(lazyhash = true)
@JsonSerialize(as = ImmutableKeyWithBytes.class)
@JsonDeserialize(as = ImmutableKeyWithBytes.class)
public interface KeyWithBytes {
  Key getKey();

  byte getType();

  ByteString getValue();

  static KeyWithBytes of(Key key, byte type, ByteString value) {
    return ImmutableKeyWithBytes.builder().key(key).type(type).value(value).build();
  }
}

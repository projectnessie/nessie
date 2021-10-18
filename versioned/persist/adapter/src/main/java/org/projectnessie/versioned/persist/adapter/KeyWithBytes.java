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

import com.google.protobuf.ByteString;
import org.immutables.value.Value;
import org.projectnessie.versioned.Key;

/** Composite of key, contents-id, contents-type and contents. */
@Value.Immutable
public interface KeyWithBytes {
  Key getKey();

  ContentsId getContentsId();

  byte getType();

  ByteString getValue();

  static KeyWithBytes of(Key key, ContentsId contentsId, byte type, ByteString value) {
    return ImmutableKeyWithBytes.builder()
        .key(key)
        .contentsId(contentsId)
        .type(type)
        .value(value)
        .build();
  }

  default KeyWithType asKeyWithType() {
    return KeyWithType.of(getKey(), getContentsId(), getType());
  }
}

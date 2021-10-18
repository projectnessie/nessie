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

import org.immutables.value.Value;
import org.projectnessie.versioned.Key;

/** Composite of key, contents-id, contents-type. */
@Value.Immutable(lazyhash = true) // this type is used as a map-key in an expensive test
public interface KeyWithType {
  Key getKey();

  ContentsId getContentsId();

  byte getType();

  static KeyWithType of(Key key, ContentsId contentsId, byte type) {
    return ImmutableKeyWithType.builder().key(key).type(type).contentsId(contentsId).build();
  }
}

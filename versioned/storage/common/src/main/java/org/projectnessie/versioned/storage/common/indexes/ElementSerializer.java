/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.indexes;

import java.nio.ByteBuffer;

public interface ElementSerializer<V> {
  int serializedSize(V value);

  /** Serialize {@code value} into {@code target}, returns {@code target}. */
  ByteBuffer serialize(V value, ByteBuffer target);

  /**
   * Deserialize a value from {@code buffer}. Implementations must not assume that the given {@link
   * ByteBuffer} only contains data for the value to deserialize, other data likely follows.
   */
  V deserialize(ByteBuffer buffer);
}

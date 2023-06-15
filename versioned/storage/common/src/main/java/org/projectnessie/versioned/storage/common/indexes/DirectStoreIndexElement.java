/*
 * Copyright (C) 2023 Dremio
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

final class DirectStoreIndexElement<V> extends AbstractStoreIndexElement<V> {
  private final StoreKey key;
  private final V content;

  DirectStoreIndexElement(StoreKey key, V content) {
    this.key = key;
    this.content = content;
  }

  @Override
  public StoreKey key() {
    return key;
  }

  @Override
  public V content() {
    return content;
  }

  @Override
  public void serializeContent(ElementSerializer<V> ser, ByteBuffer target) {
    ser.serialize(content, target);
  }

  @Override
  public int contentSerializedSize(ElementSerializer<V> ser) {
    return ser.serializedSize(content);
  }
}

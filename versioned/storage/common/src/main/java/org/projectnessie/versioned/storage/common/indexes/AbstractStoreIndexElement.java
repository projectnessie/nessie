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

import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;

abstract class AbstractStoreIndexElement<V> implements StoreIndexElement<V> {

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StoreIndexElement)) {
      return false;
    }
    if (o == this) {
      return true;
    }
    StoreIndexElement<?> other = (StoreIndexElement<?>) o;
    return key().equals(other.key()) && content().equals(other.content());
  }

  @Override
  public int hashCode() {
    @Var int h = 5381;
    h += (h << 5) + key().hashCode();
    h += (h << 5) + content().hashCode();
    return h;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper("StoreIndexElement")
        .omitNullValues()
        .add("key", key())
        .add("content", content())
        .toString();
  }
}

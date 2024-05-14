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
package org.projectnessie.versioned.storage.common.objtypes;

import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.persist.Hashable;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjIdHasher;

@Value.Immutable
public interface IndexStripe extends Hashable {

  @Value.Parameter(order = 1)
  StoreKey firstKey();

  @Value.Parameter(order = 2)
  StoreKey lastKey();

  @Value.Parameter(order = 3)
  ObjId segment();

  static IndexStripe indexStripe(StoreKey firstKey, StoreKey lastKey, ObjId segment) {
    return ImmutableIndexStripe.of(firstKey, lastKey, segment);
  }

  @Override
  default void hash(ObjIdHasher idHasher) {
    idHasher
        .hash(firstKey().rawString())
        .hash(lastKey().rawString())
        .hash(segment().asByteBuffer());
  }
}

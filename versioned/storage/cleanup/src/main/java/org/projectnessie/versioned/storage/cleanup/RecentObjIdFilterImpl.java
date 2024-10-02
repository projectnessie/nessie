/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_LINKED_HASH_MAP;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_LINKED_HASH_MAP_ENTRY;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_OBJ_ID;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_POINTER;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_PRIMITIVE_LONG_ARRAY;

import java.util.LinkedHashMap;
import java.util.Map;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class RecentObjIdFilterImpl implements RecentObjIdFilter {
  private static final Object PRESENT = new Object();

  private final LinkedHashMap<ObjId, Object> recentObjIds;
  private final long estimatedHeapPressure;

  public RecentObjIdFilterImpl(int recentObjIdsFilterSize) {
    int capacity = (int) Math.ceil(recentObjIdsFilterSize / 0.75d);
    this.estimatedHeapPressure = calculateEstimatedHeapPressure(recentObjIdsFilterSize, capacity);

    this.recentObjIds =
        new LinkedHashMap<>(capacity) {
          @Override
          protected boolean removeEldestEntry(Map.Entry<ObjId, Object> eldest) {
            return size() >= recentObjIdsFilterSize;
          }
        };
  }

  @Override
  public boolean contains(ObjId id) {
    return recentObjIds.containsKey(id);
  }

  @Override
  public boolean add(ObjId id) {
    return recentObjIds.put(id, PRESENT) == null;
  }

  @Override
  public long estimatedHeapPressure() {
    return estimatedHeapPressure;
  }

  private long calculateEstimatedHeapPressure(int size, int capacity) {
    int tableSize = -1 >>> Integer.numberOfLeadingZeros(capacity - 1);

    return HEAP_SIZE_LINKED_HASH_MAP
        +
        // LHM entries
        (HEAP_SIZE_LINKED_HASH_MAP_ENTRY + HEAP_SIZE_OBJ_ID) * size
        // LHM table/node-array
        + HEAP_SIZE_PRIMITIVE_LONG_ARRAY
        + HEAP_SIZE_POINTER * tableSize;
  }
}

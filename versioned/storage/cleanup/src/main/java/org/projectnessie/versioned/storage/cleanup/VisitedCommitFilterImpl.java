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

import static org.agrona.collections.Hashing.DEFAULT_LOAD_FACTOR;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_OBJECT_HASH_SET;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_OBJ_ID;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_PRIMITIVE_OBJ_ARRAY;

import org.agrona.collections.ObjectHashSet;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class VisitedCommitFilterImpl implements VisitedCommitFilter {
  private final ObjectHashSet<ObjId> visited = new ObjectHashSet<>(64, DEFAULT_LOAD_FACTOR);

  @Override
  public boolean mustVisit(ObjId commitObjId) {
    return visited.add(commitObjId);
  }

  @Override
  public boolean alreadyVisited(ObjId commitObjId) {
    return visited.contains(commitObjId);
  }

  @Override
  public long estimatedHeapPressure() {
    var sz = visited.size();
    var cap = visited.capacity();
    return HEAP_SIZE_OBJECT_HASH_SET + HEAP_SIZE_PRIMITIVE_OBJ_ARRAY * cap + HEAP_SIZE_OBJ_ID * sz;
  }
}

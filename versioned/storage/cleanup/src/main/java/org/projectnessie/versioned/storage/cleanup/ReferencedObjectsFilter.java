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

import jakarta.validation.constraints.NotNull;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Mark {@linkplain ObjId object IDs} as referenced and allow checking whether object IDs are marked
 * as referenced.
 *
 * <p>The implementation is usually backed by a probabilistic data structure (bloom filter), which
 * means that there is a {@linkplain #expectedFpp() chance} that an unreferenced object is not
 * collected, but all referenced objects are guaranteed to remain.
 */
public interface ReferencedObjectsFilter {
  boolean markReferenced(@NotNull ObjId objId);

  boolean isProbablyReferenced(@NotNull ObjId objId);

  boolean withinExpectedFpp();

  long approximateElementCount();

  double expectedFpp();

  long estimatedHeapPressure();
}

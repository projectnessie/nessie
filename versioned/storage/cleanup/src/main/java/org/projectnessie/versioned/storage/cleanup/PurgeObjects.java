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

public interface PurgeObjects {
  PurgeResult purge();

  /** Return the current statistics, returns a result after {@link #purge()} threw an exception. */
  PurgeStats getStats();

  /**
   * Returns the <em>estimated maximum</em> heap pressure of this object tree. Considers the data
   * structured that are required for the purge operation to work, a subset of the structures
   * required for {@link ReferencedObjectsResolver#resolve()}. It is wrong to use the sum of {@link
   * ReferencedObjectsResolver#estimatedHeapPressure()} and this value.
   */
  long estimatedHeapPressure();
}

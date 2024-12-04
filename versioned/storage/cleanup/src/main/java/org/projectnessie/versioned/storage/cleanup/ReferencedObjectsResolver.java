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

import org.projectnessie.versioned.storage.common.persist.Persist;

public interface ReferencedObjectsResolver {
  /**
   * Identifies all referenced objects in the {@linkplain Persist Nessie repository}.
   *
   * @return result containing the information for the follow-up {@linkplain PurgeObjects#purge()
   *     purge operation} and stats.
   * @throws MustRestartWithBiggerFilterException thrown if this operation identifies more than the
   *     configured {@linkplain CleanupParams#expectedObjCount() expected object count}. This
   *     exception <em>must</em> be handled by calling code
   */
  ResolveResult resolve() throws MustRestartWithBiggerFilterException;

  /**
   * Return the current statistics, returns a valid result, even if {@link #resolve()} threw an
   * exception.
   */
  ResolveStats getStats();

  /**
   * Returns the <em>estimated maximum</em> heap pressure of this object tree. Considers the data
   * structured that are required for the resolve operation to work, a superset of the structures
   * required for {@link PurgeObjects#purge()}. It is wrong to use the sum of {@link
   * PurgeObjects#estimatedHeapPressure()} and this value.
   */
  long estimatedHeapPressure();
}

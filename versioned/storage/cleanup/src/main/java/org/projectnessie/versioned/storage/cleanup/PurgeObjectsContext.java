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
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Holds the data structures and parameters that are needed for the {@linkplain PurgeObjects purge
 * operation}.
 *
 * <p>Once the {@linkplain ReferencedObjectsResolver referenced objects have been resolved}, the
 * data structures that are not needed for the purge operation should become eligible for Java GC,
 * which is why this context object exists and holds less information than {@link
 * ReferencedObjectsContext}.
 */
@NessieImmutable
public interface PurgeObjectsContext {
  @NotNull
  Persist persist();

  @NotNull
  ReferencedObjectsFilter referencedObjects();

  @NotNull
  PurgeFilter purgeFilter();

  int scanObjRatePerSecond();

  int deleteObjRatePerSecond();

  static PurgeObjectsContext purgeObjectsContext(
      ReferencedObjectsContext referencedObjectsContext) {
    return ImmutablePurgeObjectsContext.of(
        referencedObjectsContext.persist(),
        referencedObjectsContext.referencedObjects(),
        referencedObjectsContext.purgeFilter(),
        referencedObjectsContext.params().purgeScanObjRatePerSecond(),
        referencedObjectsContext.params().purgeDeleteObjRatePerSecond(),
        referencedObjectsContext.params().dryRun());
  }

  boolean dryRun();
}

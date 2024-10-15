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

import static org.projectnessie.versioned.storage.cleanup.VisitedCommitFilter.ALLOW_DUPLICATE_TRAVERSALS;

import jakarta.validation.constraints.NotNull;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Holds the data structures and parameters that are needed to {@linkplain ReferencedObjectsResolver
 * resolving referenced objects}.
 */
@NessieImmutable
public interface ReferencedObjectsContext {
  @NotNull
  Persist persist();

  @NotNull
  ReferencedObjectsFilter referencedObjects();

  @NotNull
  CleanupParams params();

  @NotNull
  PurgeFilter purgeFilter();

  @NotNull
  VisitedCommitFilter visitedCommitFilter();

  static ReferencedObjectsContext objectsResolverContext(
      Persist persist,
      CleanupParams params,
      ReferencedObjectsFilter referencedObjects,
      PurgeFilter purgeFilter) {
    return ImmutableReferencedObjectsContext.of(
        persist,
        referencedObjects,
        params,
        purgeFilter,
        params.allowDuplicateCommitTraversals()
            ? ALLOW_DUPLICATE_TRAVERSALS
            : new VisitedCommitFilterImpl());
  }
}

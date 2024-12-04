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
import java.util.List;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.versioned.storage.common.persist.Obj;

/** Filter to decide whether an {@link Obj} must be kept or whether it can be deleted. */
public interface PurgeFilter {
  boolean mustKeep(@NotNull Obj obj);

  @NessieImmutable
  interface CompositePurgeFilter extends PurgeFilter {
    List<PurgeFilter> filters();

    static CompositePurgeFilter compositePurgeFilter(PurgeFilter... filters) {
      return ImmutableCompositePurgeFilter.of(List.of(filters));
    }

    static CompositePurgeFilter compositePurgeFilter(List<PurgeFilter> filters) {
      return ImmutableCompositePurgeFilter.of(filters);
    }

    @Override
    default boolean mustKeep(Obj obj) {
      for (PurgeFilter filter : filters()) {
        if (filter.mustKeep(obj)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Recommended default purge filter, which considers a {@link ReferencedObjectsFilter} and a
   * maximum value of {@link Obj#referenced()}.
   */
  @NessieImmutable
  interface ReferencedObjectsPurgeFilter extends PurgeFilter {
    ReferencedObjectsFilter referencedObjects();

    long maxObjReferencedInMicrosSinceEpoch();

    static ReferencedObjectsPurgeFilter referencedObjectsPurgeFilter(
        ReferencedObjectsFilter referencedObjects, long maxObjReferencedInMicrosSinceEpoch) {
      return ImmutableReferencedObjectsPurgeFilter.of(
          referencedObjects, maxObjReferencedInMicrosSinceEpoch);
    }

    @Override
    default boolean mustKeep(Obj obj) {
      return obj.referenced() > maxObjReferencedInMicrosSinceEpoch()
          || referencedObjects().isProbablyReferenced(obj.id());
    }
  }
}

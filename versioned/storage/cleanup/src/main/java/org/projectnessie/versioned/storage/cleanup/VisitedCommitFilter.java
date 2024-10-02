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

import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Filter to prevent processing the same {@linkplain CommitObj Nessie commit} more than once.
 *
 * <p>There are two implementations of this interface: {@linkplain #ALLOW_DUPLICATE_TRAVERSALS one}
 * that does <em>not</em> prevent duplicate processing, and {@linkplain VisitedCommitFilterImpl the
 * default one} that does. The parameter {@link CleanupParams#allowDuplicateCommitTraversals()} is
 * used to decide which implementation is being used.
 */
public interface VisitedCommitFilter {
  boolean mustVisit(ObjId commitObjId);

  boolean alreadyVisited(ObjId commitObjId);

  long estimatedHeapPressure();

  VisitedCommitFilter ALLOW_DUPLICATE_TRAVERSALS =
      new VisitedCommitFilter() {
        @Override
        public boolean mustVisit(ObjId commitObjId) {
          return true;
        }

        @Override
        public boolean alreadyVisited(ObjId commitObjId) {
          return false;
        }

        @Override
        public long estimatedHeapPressure() {
          return 0;
        }
      };
}

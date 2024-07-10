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
package org.projectnessie.versioned.transfer.related;

import static java.util.Collections.emptySet;

import java.util.Set;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;

/**
 * Implementations identify object IDs that are needed for a complete export by providing {@link
 * ObjId}s for the repository, for {@link CommitObj}s, for {@link Content}s and {@link Reference}s.
 *
 * <p>The {@link ObjId}s returned by these functions do not need to point to existing objects. In
 * other words: it is fine to return IDs that do not exist.
 */
public interface TransferRelatedObjects {
  default Set<ObjId> repositoryRelatedObjects() {
    return emptySet();
  }

  default Set<ObjId> commitRelatedObjects(CommitObj commitObj) {
    return emptySet();
  }

  default Set<ObjId> contentRelatedObjects(Content content) {
    return emptySet();
  }

  default Set<ObjId> referenceRelatedObjects(Reference reference) {
    return emptySet();
  }
}

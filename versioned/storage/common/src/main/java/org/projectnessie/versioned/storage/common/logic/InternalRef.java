/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.persist.Reference.INTERNAL_PREFIX;

import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.RefObj;

/**
 * Constant definitions of internal, well-known references.
 *
 * <p>Internal references shall never be visible to users.
 *
 * <p>Internal references will not be present in {@link #REF_REFS}.
 */
@Value.Immutable
public interface InternalRef {

  @Value.Parameter(order = 1)
  String name();

  static InternalRef internalReference(String name) {
    return ImmutableInternalRef.of(INTERNAL_PREFIX + name);
  }

  /**
   * Internal reference that contains all known reference names, pointing to {@link RefObj} pointing
   * to their <em>initial pointers</em> and contain the timestamp when the reference has been
   * created.
   *
   * <p>Old commits in this reference might be pruned, because the history is technically not
   * required. On the other hand, the history might serve the legit use case to track who
   * created/deleted a reference and when that happened.
   */
  InternalRef REF_REFS = internalReference("refs");

  /**
   * Internal reference that points to the current {@link RepositoryDescription}.
   *
   * <p>The actual information is available via {@link #KEY_REPO_DESCRIPTION} from the HEAD commit.
   */
  InternalRef REF_REPO = internalReference("repo");

  static List<InternalRef> allInternalRefs() {
    return List.of(REF_REPO, REF_REFS);
  }

  StoreKey KEY_REPO_DESCRIPTION = key("repo", "description");
}

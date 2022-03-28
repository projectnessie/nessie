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
package org.projectnessie.services.authz;

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.NamedRef;

/** Describes a check operation. */
@Value.Immutable
public interface Check {
  CheckType type();

  @Nullable
  NamedRef ref();

  @Nullable
  ContentKey key();

  @Nullable
  String contentId();

  @Nullable
  Content.Type contentType();

  static ImmutableCheck.Builder builder(CheckType type) {
    return ImmutableCheck.builder().type(type);
  }

  enum CheckType {
    /** See {@link BatchAccessChecker#canViewReference(NamedRef)}. */
    VIEW_REFERENCE(true, false),
    /** See {@link BatchAccessChecker#canCreateReference(NamedRef)}. */
    CREATE_REFERENCE(true, false),
    /** See {@link BatchAccessChecker#canAssignRefToHash(NamedRef)}. */
    ASSIGN_REFERENCE_TO_HASH(true, false),
    /** See {@link BatchAccessChecker#canDeleteReference(NamedRef)}. */
    DELETE_REFERENCE(true, false),
    /** See {@link BatchAccessChecker#canDeleteDefaultBranch()}. */
    DELETE_DEFAULT_BRANCH(false, false),
    /** See {@link BatchAccessChecker#canReadEntries(NamedRef)}. */
    READ_ENTRIES(true, false),
    /** See {@link BatchAccessChecker#canReadContentKey(NamedRef, ContentKey, String)}. */
    READ_CONTENT_KEY(true, true),
    /** See {@link BatchAccessChecker#canListCommitLog(NamedRef)}. */
    LIST_COMMIT_LOG(true, false),
    /** See {@link BatchAccessChecker#canCommitChangeAgainstReference(NamedRef)}. */
    COMMIT_CHANGE_AGAINST_REFERENCE(true, false),
    /** See {@link BatchAccessChecker#canReadEntityValue(NamedRef, ContentKey, String)}. */
    READ_ENTITY_VALUE(true, true),
    /**
     * See {@link BatchAccessChecker#canUpdateEntity(NamedRef, ContentKey, String, Content.Type)}.
     */
    UPDATE_ENTITY(true, true),
    /** See {@link BatchAccessChecker#canDeleteEntity(NamedRef, ContentKey, String)}. */
    DELETE_ENTITY(true, true),
    /** See {@link BatchAccessChecker#canViewRefLog()}. */
    VIEW_REFLOG(false, false);

    private final boolean ref;
    private final boolean content;

    CheckType(boolean ref, boolean content) {
      this.ref = ref;
      this.content = content;
    }

    public boolean isRef() {
      return ref;
    }

    public boolean isContent() {
      return content;
    }
  }
}

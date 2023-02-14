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
  @Value.Parameter(order = 1)
  CheckType type();

  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 2)
  NamedRef ref();

  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 3)
  ContentKey key();

  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 4)
  String contentId();

  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 5)
  Content.Type contentType();

  static Check check(CheckType type) {
    return check(type, null);
  }

  static Check check(CheckType type, @Nullable @jakarta.annotation.Nullable NamedRef ref) {
    return check(type, ref, null, null);
  }

  static Check check(
      CheckType type,
      @Nullable @jakarta.annotation.Nullable NamedRef ref,
      @Nullable @jakarta.annotation.Nullable ContentKey key,
      @Nullable @jakarta.annotation.Nullable String contentId) {
    return check(type, ref, key, contentId, null);
  }

  static Check check(
      CheckType type,
      @Nullable @jakarta.annotation.Nullable NamedRef ref,
      @Nullable @jakarta.annotation.Nullable ContentKey key,
      @Nullable @jakarta.annotation.Nullable String contentId,
      @Nullable @jakarta.annotation.Nullable Content.Type contentType) {
    return ImmutableCheck.of(type, ref, key, contentId, contentType);
  }

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

  static Check canViewReference(NamedRef ref) {
    return check(CheckType.VIEW_REFERENCE, ref);
  }

  static Check canCreateReference(NamedRef ref) {
    return check(CheckType.CREATE_REFERENCE, ref);
  }

  static Check canAssignRefToHash(NamedRef ref) {
    return check(CheckType.ASSIGN_REFERENCE_TO_HASH, ref);
  }

  static Check canDeleteReference(NamedRef ref) {
    return check(CheckType.DELETE_REFERENCE, ref);
  }

  static Check canReadEntries(NamedRef ref) {
    return check(CheckType.READ_ENTRIES, ref);
  }

  static Check canReadContentKey(NamedRef ref, ContentKey key, String contentId) {
    return check(CheckType.READ_CONTENT_KEY, ref, key, contentId);
  }

  static Check canListCommitLog(NamedRef ref) {
    return check(CheckType.LIST_COMMIT_LOG, ref);
  }

  static Check canCommitChangeAgainstReference(NamedRef ref) {
    return check(CheckType.COMMIT_CHANGE_AGAINST_REFERENCE, ref);
  }

  static Check canReadEntityValue(NamedRef ref, ContentKey key, String contentId) {
    return check(CheckType.READ_ENTITY_VALUE, ref, key, contentId);
  }

  static Check canUpdateEntity(
      NamedRef ref, ContentKey key, String contentId, Content.Type contentType) {
    return check(CheckType.UPDATE_ENTITY, ref, key, contentId, contentType);
  }

  static Check canDeleteEntity(NamedRef ref, ContentKey key, String contentId) {
    return check(CheckType.DELETE_ENTITY, ref, key, contentId);
  }

  static Check canViewRefLog() {
    return check(CheckType.VIEW_REFLOG);
  }
}

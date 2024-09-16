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

import static org.projectnessie.services.authz.Check.Component.ALL_SET;

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.NamedRef;

/** Describes a check operation. */
@Value.Immutable
public interface Check {
  @Value.Parameter(order = 1)
  CheckType type();

  @Nullable
  @Value.Parameter(order = 2)
  NamedRef ref();

  @Nullable
  @Value.Parameter(order = 3)
  ContentKey key();

  @Nullable
  @Value.Parameter(order = 4)
  String contentId();

  @Nullable
  @Value.Parameter(order = 5)
  Content.Type contentType();

  @Nullable
  @Value.Parameter(order = 6)
  IdentifiedContentKey identifiedKey();

  @Nullable
  @Value.Parameter(order = 7)
  RepositoryConfig.Type repositoryConfigType();

  @Value.Parameter(order = 8)
  Set<Component> components();

  static Check check(CheckType type) {
    return ImmutableCheck.of(type, null, null, null, null, null, null, ALL_SET);
  }

  static Check check(CheckType type, RepositoryConfig.Type repositoryConfigType) {
    return ImmutableCheck.of(type, null, null, null, null, null, repositoryConfigType, ALL_SET);
  }

  static Check check(CheckType type, @Nullable NamedRef ref) {
    return ImmutableCheck.of(type, ref, null, null, null, null, null, ALL_SET);
  }

  static Check check(CheckType type, @Nullable NamedRef ref, @Nullable ContentKey key) {
    return ImmutableCheck.of(type, ref, key, null, null, null, null, ALL_SET);
  }

  static Check check(
      CheckType type, @Nullable NamedRef ref, @Nullable IdentifiedContentKey identifiedKey) {
    return check(type, ref, identifiedKey, ALL_SET);
  }

  static Check check(
      @NotNull CheckType type,
      @NotNull NamedRef ref,
      @NotNull IdentifiedContentKey identifiedKey,
      @Nullable Collection<Component> components) {
    Set<Component> componentSet =
        components == null
            ? ALL_SET
            : (components instanceof Set ? (Set<Component>) components : Set.copyOf(components));
    if (identifiedKey != null) {
      IdentifiedContentKey.IdentifiedElement element = identifiedKey.lastElement();
      return ImmutableCheck.of(
          type,
          ref,
          identifiedKey.contentKey(),
          element.contentId(),
          identifiedKey.type(),
          identifiedKey,
          null,
          componentSet);
    }
    return ImmutableCheck.of(type, ref, null, null, null, null, null, componentSet);
  }

  static ImmutableCheck.Builder builder(CheckType type) {
    return ImmutableCheck.builder().type(type);
  }

  enum CheckType {
    /** See {@link BatchAccessChecker#canViewReference(NamedRef)}. */
    VIEW_REFERENCE(true, false, false),
    /** See {@link BatchAccessChecker#canCreateReference(NamedRef)}. */
    CREATE_REFERENCE(true, false, false),
    /** See {@link BatchAccessChecker#canAssignRefToHash(NamedRef)}. */
    ASSIGN_REFERENCE_TO_HASH(true, false, false),
    /** See {@link BatchAccessChecker#canDeleteReference(NamedRef)}. */
    DELETE_REFERENCE(true, false, false),
    /** See {@link BatchAccessChecker#canReadEntries(NamedRef)}. */
    READ_ENTRIES(true, false, false),
    /** See {@link BatchAccessChecker#canReadContentKey(NamedRef, IdentifiedContentKey)}. */
    READ_CONTENT_KEY(true, true, false),
    /** See {@link BatchAccessChecker#canListCommitLog(NamedRef)}. */
    LIST_COMMIT_LOG(true, false, false),
    /** See {@link BatchAccessChecker#canCommitChangeAgainstReference(NamedRef)}. */
    COMMIT_CHANGE_AGAINST_REFERENCE(true, false, false),
    /** See {@link BatchAccessChecker#canReadEntityValue(NamedRef, IdentifiedContentKey, Set)}. */
    READ_ENTITY_VALUE(true, true, false),
    /** See {@link BatchAccessChecker#canCreateEntity(NamedRef, IdentifiedContentKey, Set)}. */
    CREATE_ENTITY(true, true, false),
    /** See {@link BatchAccessChecker#canUpdateEntity(NamedRef, IdentifiedContentKey, Set)}. */
    UPDATE_ENTITY(true, true, false),
    /** See {@link BatchAccessChecker#canDeleteEntity(NamedRef, IdentifiedContentKey, Set)}. */
    DELETE_ENTITY(true, true, false),

    READ_REPOSITORY_CONFIG(false, false, true),

    UPDATE_REPOSITORY_CONFIG(false, false, true),
    ;

    private final boolean ref;
    private final boolean content;
    private final boolean repositoryConfigType;

    CheckType(boolean ref, boolean content, boolean repositoryConfigType) {
      this.ref = ref;
      this.content = content;
      this.repositoryConfigType = repositoryConfigType;
    }

    public boolean isRef() {
      return ref;
    }

    public boolean isContent() {
      return content;
    }

    public boolean isRepositoryConfigType() {
      return repositoryConfigType;
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

  static Check canReadContentKey(NamedRef ref, ContentKey key) {
    return check(CheckType.READ_CONTENT_KEY, ref, key);
  }

  static Check canReadContentKey(NamedRef ref, IdentifiedContentKey identifiedKey) {
    return check(CheckType.READ_CONTENT_KEY, ref, identifiedKey);
  }

  static Check canListCommitLog(NamedRef ref) {
    return check(CheckType.LIST_COMMIT_LOG, ref);
  }

  static Check canCommitChangeAgainstReference(NamedRef ref) {
    return check(CheckType.COMMIT_CHANGE_AGAINST_REFERENCE, ref);
  }

  static Check canReadEntityValue(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<Component> components) {
    return check(CheckType.READ_ENTITY_VALUE, ref, identifiedKey, components);
  }

  static Check canCreateEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<Component> components) {
    return check(CheckType.CREATE_ENTITY, ref, identifiedKey, components);
  }

  static Check canUpdateEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<Component> components) {
    return check(CheckType.UPDATE_ENTITY, ref, identifiedKey, components);
  }

  static Check canDeleteEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<Component> components) {
    return check(CheckType.DELETE_ENTITY, ref, identifiedKey, components);
  }

  static Check canReadRepositoryConfig(RepositoryConfig.Type repositoryConfigType) {
    return check(CheckType.READ_REPOSITORY_CONFIG, repositoryConfigType);
  }

  static Check canUpdateRepositoryConfig(RepositoryConfig.Type repositoryConfigType) {
    return check(CheckType.UPDATE_REPOSITORY_CONFIG, repositoryConfigType);
  }

  final class Component {
    private static final Map<String, Component> REGISTERED = new ConcurrentHashMap<>();

    private final String name;

    public static final Component ALL = part("ALL");
    public static final Set<Component> ALL_SET = Set.of(ALL);

    private Component(String name) {
      this.name = name;
    }

    public static Component part(String name) {
      return REGISTERED.computeIfAbsent(name, Component::new);
    }

    public String name() {
      return name;
    }

    @Override
    public String toString() {
      return name;
    }
  }
}

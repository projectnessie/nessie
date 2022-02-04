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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.versioned.NamedRef;

public abstract class AbstractAccessChecker implements AccessChecker {
  public static final AccessChecker NOOP_ACCESS_CHECKER =
      new AbstractAccessChecker() {
        @Override
        public Map<Check, String> check() {
          return Collections.emptyMap();
        }
      };

  private final Collection<Check> checks = new LinkedHashSet<>();

  private AccessChecker add(ImmutableCheck.Builder builder) {
    checks.add(builder.build());
    return this;
  }

  protected Collection<Check> getChecks() {
    return checks;
  }

  @Override
  public AccessChecker canViewReference(NamedRef ref) {
    return add(Check.builder(CheckType.VIEW_REFERENCE).ref(ref));
  }

  @Override
  public AccessChecker canCreateReference(NamedRef ref) {
    return add(Check.builder(CheckType.CREATE_REFERENCE).ref(ref));
  }

  @Override
  public AccessChecker canAssignRefToHash(NamedRef ref) {
    canViewReference(ref);
    return add(Check.builder(CheckType.ASSIGN_REFERENCE_TO_HASH).ref(ref));
  }

  @Override
  public AccessChecker canDeleteReference(NamedRef ref) {
    canViewReference(ref);
    return add(Check.builder(CheckType.DELETE_REFERENCE).ref(ref));
  }

  @Override
  public AccessChecker canReadEntries(NamedRef ref) {
    canViewReference(ref);
    return add(Check.builder(CheckType.READ_ENTRIES).ref(ref));
  }

  @Override
  public AccessChecker canListCommitLog(NamedRef ref) {
    canViewReference(ref);
    return add(Check.builder(CheckType.LIST_COMMIT_LOG).ref(ref));
  }

  @Override
  public AccessChecker canCommitChangeAgainstReference(NamedRef ref) {
    canViewReference(ref);
    return add(Check.builder(CheckType.COMMIT_CHANGE_AGAINST_REFERENCE).ref(ref));
  }

  @Override
  public AccessChecker canReadEntityValue(NamedRef ref, ContentKey key, String contentId) {
    canViewReference(ref);
    return add(Check.builder(CheckType.READ_ENTITY_VALUE).ref(ref).key(key).contentId(contentId));
  }

  @Override
  public AccessChecker canUpdateEntity(NamedRef ref, ContentKey key, String contentId) {
    canViewReference(ref);
    return add(Check.builder(CheckType.UPDATE_ENTITY).ref(ref).key(key).contentId(contentId));
  }

  @Override
  public AccessChecker canDeleteEntity(NamedRef ref, ContentKey key, String contentId) {
    canViewReference(ref);
    return add(Check.builder(CheckType.DELETE_ENTITY).ref(ref).key(key).contentId(contentId));
  }

  @Override
  public AccessChecker canViewRefLog() {
    return add(Check.builder(CheckType.VIEW_REFLOG));
  }
}

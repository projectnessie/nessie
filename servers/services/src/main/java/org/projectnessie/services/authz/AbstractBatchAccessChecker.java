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
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.authz.Check.CheckType;
import org.projectnessie.versioned.NamedRef;

public abstract class AbstractBatchAccessChecker implements BatchAccessChecker {
  public static final BatchAccessChecker NOOP_ACCESS_CHECKER =
      new AbstractBatchAccessChecker() {
        @Override
        public Map<Check, String> check() {
          return Collections.emptyMap();
        }
      };

  private final Collection<Check> checks = new LinkedHashSet<>();

  private BatchAccessChecker add(Check check) {
    checks.add(check);
    return this;
  }

  protected Collection<Check> getChecks() {
    return checks;
  }

  @Override
  public BatchAccessChecker canViewReference(NamedRef ref) {
    return add(Check.of(CheckType.VIEW_REFERENCE, ref));
  }

  @Override
  public BatchAccessChecker canCreateReference(NamedRef ref) {
    return add(Check.of(CheckType.CREATE_REFERENCE, ref));
  }

  @Override
  public BatchAccessChecker canAssignRefToHash(NamedRef ref) {
    canViewReference(ref);
    return add(Check.of(CheckType.ASSIGN_REFERENCE_TO_HASH, ref));
  }

  @Override
  public BatchAccessChecker canDeleteReference(NamedRef ref) {
    canViewReference(ref);
    return add(Check.of(CheckType.DELETE_REFERENCE, ref));
  }

  @Override
  public BatchAccessChecker canReadEntries(NamedRef ref) {
    canViewReference(ref);
    return add(Check.of(CheckType.READ_ENTRIES, ref));
  }

  @Override
  public BatchAccessChecker canReadContentKey(NamedRef ref, ContentKey key, String contentId) {
    canViewReference(ref);
    return add(Check.of(CheckType.READ_CONTENT_KEY, ref, key, contentId));
  }

  @Override
  public BatchAccessChecker canListCommitLog(NamedRef ref) {
    canViewReference(ref);
    return add(Check.of(CheckType.LIST_COMMIT_LOG, ref));
  }

  @Override
  public BatchAccessChecker canCommitChangeAgainstReference(NamedRef ref) {
    canViewReference(ref);
    return add(Check.of(CheckType.COMMIT_CHANGE_AGAINST_REFERENCE, ref));
  }

  @Override
  public BatchAccessChecker canReadEntityValue(NamedRef ref, ContentKey key, String contentId) {
    canViewReference(ref);
    return add(Check.of(CheckType.READ_ENTITY_VALUE, ref, key, contentId));
  }

  @Override
  public BatchAccessChecker canUpdateEntity(
      NamedRef ref, ContentKey key, String contentId, Content.Type contentType) {
    canViewReference(ref);
    return add(Check.of(CheckType.UPDATE_ENTITY, ref, key, contentId, contentType));
  }

  @Override
  public BatchAccessChecker canDeleteEntity(NamedRef ref, ContentKey key, String contentId) {
    canViewReference(ref);
    return add(Check.of(CheckType.DELETE_ENTITY, ref, key, contentId));
  }

  @Override
  public BatchAccessChecker canViewRefLog() {
    return add(Check.of(CheckType.VIEW_REFLOG));
  }
}

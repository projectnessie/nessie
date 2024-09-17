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

import static java.util.Collections.emptyMap;
import static org.projectnessie.services.authz.ApiContext.apiContext;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.NamedRef;

public abstract class AbstractBatchAccessChecker implements BatchAccessChecker {
  public static final BatchAccessChecker NOOP_ACCESS_CHECKER =
      new AbstractBatchAccessChecker(apiContext("<unknown>", 0)) {
        @Override
        public Map<Check, String> check() {
          return emptyMap();
        }

        @Override
        public BatchAccessChecker can(Check check) {
          return this;
        }
      };

  private final ApiContext apiContext;
  private final Collection<Check> checks = new LinkedHashSet<>();

  protected AbstractBatchAccessChecker(ApiContext apiContext) {
    this.apiContext = apiContext;
  }

  @Override
  public ApiContext getApiContext() {
    return apiContext;
  }

  protected Collection<Check> getChecks() {
    return checks;
  }

  @Override
  public BatchAccessChecker can(Check check) {
    checks.add(check);
    return this;
  }

  @Override
  public BatchAccessChecker canViewReference(NamedRef ref) {
    return can(Check.canViewReference(ref));
  }

  @Override
  public BatchAccessChecker canCreateReference(NamedRef ref) {
    return can(Check.canCreateReference(ref));
  }

  @Override
  public BatchAccessChecker canAssignRefToHash(NamedRef ref) {
    canViewReference(ref);
    return can(Check.canAssignRefToHash(ref));
  }

  @Override
  public BatchAccessChecker canDeleteReference(NamedRef ref) {
    canViewReference(ref);
    return can(Check.canDeleteReference(ref));
  }

  @Override
  public BatchAccessChecker canReadEntries(NamedRef ref) {
    canViewReference(ref);
    return can(Check.canReadEntries(ref));
  }

  @Override
  public BatchAccessChecker canReadContentKey(NamedRef ref, IdentifiedContentKey identifiedKey) {
    canViewReference(ref);
    return can(Check.canReadContentKey(ref, identifiedKey));
  }

  @Override
  public BatchAccessChecker canReadContentKey(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions) {
    canViewReference(ref);
    return can(Check.canReadContentKey(ref, identifiedKey, actions));
  }

  @Override
  public BatchAccessChecker canListCommitLog(NamedRef ref) {
    canViewReference(ref);
    return can(Check.canListCommitLog(ref));
  }

  @Override
  public BatchAccessChecker canCommitChangeAgainstReference(NamedRef ref) {
    canViewReference(ref);
    return can(Check.canCommitChangeAgainstReference(ref));
  }

  @Override
  public BatchAccessChecker canReadEntityValue(NamedRef ref, IdentifiedContentKey identifiedKey) {
    canViewReference(ref);
    return can(Check.canReadEntityValue(ref, identifiedKey));
  }

  @Override
  public BatchAccessChecker canReadEntityValue(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions) {
    canViewReference(ref);
    return can(Check.canReadEntityValue(ref, identifiedKey, actions));
  }

  @Override
  @Deprecated
  public BatchAccessChecker canCreateEntity(NamedRef ref, IdentifiedContentKey identifiedKey) {
    canViewReference(ref);
    return can(Check.canCreateEntity(ref, identifiedKey));
  }

  @Override
  @Deprecated
  public BatchAccessChecker canCreateEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions) {
    canViewReference(ref);
    return can(Check.canCreateEntity(ref, identifiedKey, actions));
  }

  @Override
  @Deprecated
  public BatchAccessChecker canUpdateEntity(NamedRef ref, IdentifiedContentKey identifiedKey) {
    canViewReference(ref);
    return can(Check.canUpdateEntity(ref, identifiedKey));
  }

  @Override
  @Deprecated
  public BatchAccessChecker canUpdateEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions) {
    canViewReference(ref);
    return can(Check.canUpdateEntity(ref, identifiedKey, actions));
  }

  @Override
  public BatchAccessChecker canDeleteEntity(NamedRef ref, IdentifiedContentKey identifiedKey) {
    canViewReference(ref);
    return can(Check.canDeleteEntity(ref, identifiedKey));
  }

  @Override
  public BatchAccessChecker canDeleteEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions) {
    canViewReference(ref);
    return can(Check.canDeleteEntity(ref, identifiedKey, actions));
  }

  @Override
  public BatchAccessChecker canReadRepositoryConfig(RepositoryConfig.Type repositoryConfigType) {
    return can(Check.canReadRepositoryConfig(repositoryConfigType));
  }

  @Override
  public BatchAccessChecker canUpdateRepositoryConfig(RepositoryConfig.Type repositoryConfigType) {
    return can(Check.canUpdateRepositoryConfig(repositoryConfigType));
  }
}

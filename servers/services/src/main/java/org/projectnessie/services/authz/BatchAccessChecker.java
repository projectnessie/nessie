/*
 * Copyright (C) 2020 Dremio
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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import java.util.Set;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.model.Tag;
import org.projectnessie.versioned.NamedRef;

/**
 * Accept various allowance-checks and retrieve the result of all operations at once.
 *
 * <p>The purpose of the {@link BatchAccessChecker} is to accept all required checks via the {@code
 * can...()} methods and return the result of these "can do xyz" checks via {@link #check()}.
 *
 * <p>The checks make sure that a particular role is allowed to perform an action (such as creation,
 * deletion) on a {@link NamedRef} (Branch/Tag). Additionally, this interface also provides checks
 * based on a given {@link ContentKey}.
 *
 * <p>It is safe to call a check method with the same arguments multiple times.
 *
 * <p>Implementations can expect that either {@link #check()} or {@link #checkAndThrow()} are called
 * either once or never.
 *
 * @see Check
 * @see Check.CheckType
 * @see AbstractBatchAccessChecker
 */
public interface BatchAccessChecker {

  /**
   * Checks the recorded checks.
   *
   * @return map of failed checks or an empty collection, if all checks passed
   */
  Map<Check, String> check();

  ApiContext getApiContext();

  /**
   * Convenience methods that throws an {@link AccessCheckException}, if {@link #check()} returns a
   * non-empty map.
   */
  default void checkAndThrow() throws AccessCheckException {
    throwForFailedChecks(check());
  }

  static void throwForFailedChecks(Map<Check, String> failedChecks) throws AccessCheckException {
    if (!failedChecks.isEmpty()) {
      throw new AccessCheckException(String.join(", ", failedChecks.values()));
    }
  }

  @CanIgnoreReturnValue
  BatchAccessChecker can(Check check);

  /**
   * Checks whether the given role/principal is allowed to view/list the given {@link Branch}/{@link
   * Tag} or {@link Detached}.
   *
   * @param ref The {@link NamedRef} to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canViewReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to create a {@link Branch}/{@link Tag}.
   *
   * @param ref The {@link NamedRef} to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canCreateReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to assign the given {@link Branch}/{@link
   * Tag} to a commit id.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check not granted.
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canAssignRefToHash(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to delete a {@link Branch}/{@link Tag}.
   *
   * @param ref The {@link NamedRef} to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canDeleteReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to read entries content for the given {@link
   * Branch}/{@link Tag} or {@link Detached}.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canReadEntries(NamedRef ref);

  /**
   * Called for every content-key about to be returned from, for example, a "get commit log"
   * operation.
   *
   * <p>This is an additional check for each content-key. "Early" checks, that run before generating
   * the result, like {@link #canReadEntries(NamedRef)} or {@link #canListCommitLog(NamedRef)}, run
   * as well.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref current reference
   * @param identifiedKey content key / ID / type to check
   * @param actions contextual information, API actions/operations performed
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canReadContentKey(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions);

  @CanIgnoreReturnValue
  BatchAccessChecker canReadContentKey(NamedRef ref, IdentifiedContentKey identifiedKey);

  /**
   * Checks whether the given role/principal is allowed to list the commit log for the given {@link
   * Branch}/{@link Tag} or {@link Detached}.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canListCommitLog(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to commit changes against the given {@link
   * Branch}/{@link Tag} or {@link Detached}.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canCommitChangeAgainstReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to read an entity value as defined by the
   * {@link ContentKey} for the given {@link Branch}/{@link Tag} or {@link Detached}.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   * @param identifiedKey content key / ID / type to check
   * @param actions contextual information, API actions/operations performed
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canReadEntityValue(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions);

  @CanIgnoreReturnValue
  BatchAccessChecker canReadEntityValue(NamedRef ref, IdentifiedContentKey identifiedKey);

  /**
   * Checks whether the given role/principal is allowed to create a new entity value as defined by
   * the {@link IdentifiedContentKey} for the given {@link Branch}, called for a {@link
   * Operation.Put} operation in a commit.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   * @param identifiedKey content key / ID / type to check
   * @param actions contextual information, API actions/operations performed
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canCreateEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions);

  @CanIgnoreReturnValue
  BatchAccessChecker canCreateEntity(NamedRef ref, IdentifiedContentKey identifiedKey);

  /**
   * Checks whether the given role/principal is allowed to update an existing entity value as
   * defined by the {@link IdentifiedContentKey} for the given {@link Branch}, called for a {@link
   * Operation.Put} operation in a commit.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   * @param identifiedKey content key / ID / type to check
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canUpdateEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions);

  @CanIgnoreReturnValue
  BatchAccessChecker canUpdateEntity(NamedRef ref, IdentifiedContentKey identifiedKey);

  /**
   * Checks whether the given role/principal is allowed to delete an entity value as defined by the
   * {@link ContentKey} for the given {@link Branch}, called for a {@link Operation.Delete}
   * operation in a commit.
   *
   * <p>Adds an implicit {@link #canViewReference(NamedRef)}.
   *
   * @param ref The {@link NamedRef} to check
   * @param identifiedKey content key / ID / type to check
   * @param actions contextual information, API actions/operations performed
   */
  @CanIgnoreReturnValue
  BatchAccessChecker canDeleteEntity(
      NamedRef ref, IdentifiedContentKey identifiedKey, Set<String> actions);

  @CanIgnoreReturnValue
  BatchAccessChecker canDeleteEntity(NamedRef ref, IdentifiedContentKey identifiedKey);

  @CanIgnoreReturnValue
  BatchAccessChecker canReadRepositoryConfig(RepositoryConfig.Type repositoryConfigType);

  @CanIgnoreReturnValue
  BatchAccessChecker canUpdateRepositoryConfig(RepositoryConfig.Type repositoryConfigType);
}

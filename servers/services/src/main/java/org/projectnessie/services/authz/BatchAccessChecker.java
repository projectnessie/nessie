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

import java.security.AccessControlException;
import java.util.Map;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
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
 */
public interface BatchAccessChecker {

  /**
   * Checks the recorded checks.
   *
   * @return set of failed checks or an empty collection, if all checks passed
   */
  Map<Check, String> check();

  /**
   * Convenience methods that throws an {@link AccessControlException}, if {@link #check()} returns
   * a non-empty map.
   */
  default void checkAndThrow() throws AccessControlException {
    Map<Check, String> failedChecks = check();
    if (!failedChecks.isEmpty()) {
      throw new AccessControlException(String.join(", ", failedChecks.values()));
    }
  }

  /**
   * Checks whether the given role/principal is allowed to view/list the given Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   */
  BatchAccessChecker canViewReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to create a Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   */
  BatchAccessChecker canCreateReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to assign the given Branch/Tag to a Hash.
   *
   * @param ref The {@link NamedRef} to check not granted.
   */
  BatchAccessChecker canAssignRefToHash(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to delete a Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   */
  BatchAccessChecker canDeleteReference(NamedRef ref);

  /** Checks whether the given role/principal is allowed to delete the default branch. */
  BatchAccessChecker canDeleteDefaultBranch();

  /**
   * Checks whether the given role/principal is allowed to read entries content for the given
   * Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   */
  BatchAccessChecker canReadEntries(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to list the commit log for the given
   * Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   */
  BatchAccessChecker canListCommitLog(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to commit changes against the given
   * Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   */
  BatchAccessChecker canCommitChangeAgainstReference(NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to read an entity value as defined by the
   * {@link ContentKey} for the given Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   * @param key The {@link ContentKey} to check
   * @param contentId The ID of the {@link Content} object. See the <a
   *     href="https://projectnessie.org/features/metadata_authorization/#contentid">ContentId
   *     docs</a> for how to use this.
   */
  BatchAccessChecker canReadEntityValue(NamedRef ref, ContentKey key, String contentId);

  /**
   * Checks whether the given role/principal is allowed to update an entity value as defined by the
   * {@link ContentKey} for the given Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   * @param key The {@link ContentKey} to check
   * @param contentId The ID of the {@link Content} object. See the <a
   *     href="https://projectnessie.org/features/metadata_authorization/#contentid">ContentId
   *     docs</a> for how to use this.
   */
  BatchAccessChecker canUpdateEntity(NamedRef ref, ContentKey key, String contentId);

  /**
   * Checks whether the given role/principal is allowed to delete an entity value as defined by the
   * {@link ContentKey} for the given Branch/Tag.
   *
   * @param ref The {@link NamedRef} to check
   * @param key The {@link ContentKey} to check
   * @param contentId The ID of the {@link Content} object. See the <a
   *     href="https://projectnessie.org/features/metadata_authorization/#contentid">ContentId
   *     docs</a> for how to use this.
   */
  BatchAccessChecker canDeleteEntity(NamedRef ref, ContentKey key, String contentId);

  /** Checks whether the given role/principal is allowed to view the reflog entries. */
  BatchAccessChecker canViewRefLog();
}

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

import org.projectnessie.model.ContentsKey;
import org.projectnessie.versioned.NamedRef;

/**
 * The purpose of the {@link AccessChecker} is to make sure that a particular role is allowed to
 * perform an action (such as creation, deletion) on a {@link NamedRef} (Branch/Tag). Additionally,
 * this interface also provides checks based on a given {@link ContentsKey}.
 */
public interface AccessChecker {

  /**
   * Checks whether the given role/principal is allowed to create a Branch/Tag. It is up to the
   * implementor to define which exception should being thrown in case of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   */
  void canCreateReference(AccessContext context, NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to assign the given Branch/Tag to a Hash. It
   * is up to the implementor to define which exception should being thrown in case of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   */
  void canAssignRefToHash(AccessContext context, NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to delete a Branch/Tag. It is up to the
   * implementor to define which exception should being thrown in case of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   */
  void canDeleteReference(AccessContext context, NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to read entries content for the given
   * Branch/Tag. It is up to the implementor to define which exception should being thrown in case
   * of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   */
  void canReadEntries(AccessContext context, NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to list the commit log for the given
   * Branch/Tag. It is up to the implementor to define which exception should being thrown in case
   * of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   */
  void canListCommitLog(AccessContext context, NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to commit changes against the given
   * Branch/Tag. It is up to the implementor to define which exception should being thrown in case
   * of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   */
  void canCommitChangeAgainstReference(AccessContext context, NamedRef ref);

  /**
   * Checks whether the given role/principal is allowed to read an entity value as defined by the
   * {@link ContentsKey} for the given Branch/Tag. It is up to the implementor to define which
   * exception should being thrown in case of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   * @param key The {@link ContentsKey} to check
   */
  void canReadEntityValue(AccessContext context, NamedRef ref, ContentsKey key);

  /**
   * Checks whether the given role/principal is allowed to update an entity value as defined by the
   * {@link ContentsKey} for the given Branch/Tag. It is up to the implementor to define which
   * exception should being thrown in case of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   * @param key The {@link ContentsKey} to check
   */
  void canUpdateEntity(AccessContext context, NamedRef ref, ContentsKey key);

  /**
   * Checks whether the given role/principal is allowed to delete an entity value as defined by the
   * {@link ContentsKey} for the given Branch/Tag. It is up to the implementor to define which
   * exception should being thrown in case of denial.
   *
   * @param context The context carrying the principal information.
   * @param ref The {@link NamedRef} to check
   * @param key The {@link ContentsKey} to check
   */
  void canDeleteEntity(AccessContext context, NamedRef ref, ContentsKey key);
}

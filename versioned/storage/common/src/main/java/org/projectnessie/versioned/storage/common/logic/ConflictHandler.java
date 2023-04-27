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

import org.projectnessie.versioned.storage.common.logic.CommitLogic.ValueReplacement;

@FunctionalInterface
public interface ConflictHandler {

  ConflictResolution onConflict(CommitConflict conflict);

  /**
   * See also {@link CommitLogic#buildCommitObj(CreateCommit, ConflictHandler, CommitOpHandler,
   * ValueReplacement, ValueReplacement)}.
   */
  enum ConflictResolution {
    /** Raise the conflict as is. */
    CONFLICT,
    /** Ignore the conflict, add action to the commit. */
    ADD,
    /** Ignore the conflict, do not add the action to the commit. */
    DROP
  }
}

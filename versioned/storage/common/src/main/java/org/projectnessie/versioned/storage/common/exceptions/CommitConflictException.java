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
package org.projectnessie.versioned.storage.common.exceptions;

import java.util.List;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.common.logic.CommitConflict;

public class CommitConflictException extends CommitException {

  private final List<CommitConflict> conflicts;

  public CommitConflictException(List<CommitConflict> conflicts) {
    super("Commit conflict");
    this.conflicts = conflicts;
  }

  @Override
  public String getMessage() {
    return "Commit conflict: "
        + conflicts.stream()
            .map(c -> c.conflictType().name() + ":" + c.key().toString())
            .collect(Collectors.joining(", "));
  }

  public List<CommitConflict> conflicts() {
    return conflicts;
  }
}

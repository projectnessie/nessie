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
package org.projectnessie.versioned;

import java.util.List;
import org.immutables.value.Value;

/** Represents a reflog-entry stored in the database. */
@Value.Immutable
public interface RefLogDetails {

  /** Reflog id of the current entry. */
  Hash getRefLogId();

  /** Reference on which current operation is executed. */
  String getRefName();

  /** Reference type can be 'Branch' or 'Tag'. */
  String getRefType();

  /** Output commit hash of the operation. */
  Hash getCommitHash();

  /** Parent reflog id of the current entry. */
  Hash getParentRefLogId();

  /** Time in microseconds since epoch. */
  long getOperationTime();

  /** Operation String mapped to ENUM in {@code RefLogEntry.Operation} of 'persist.proto' file. */
  String getOperation();

  /**
   * Single hash in case of MERGE or ASSIGN. One or more hashes in case of TRANSPLANT. Empty list
   * for other operations.
   */
  List<Hash> getSourceHashes();
}

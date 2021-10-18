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
package org.projectnessie.versioned.persist.adapter;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;

/**
 * API helper method to encapsulate parameters for {@link DatabaseAdapter#commit(CommitAttempt)}.
 */
@Value.Immutable
public interface CommitAttempt {

  /**
   * Branch to commit to. If {@link #getExpectedHead()} is present, the referenced branch's HEAD
   * must be equal to this hash.
   */
  BranchName getCommitToBranch();

  /** Expected HEAD of {@link #getCommitToBranch()}. */
  @Value.Default
  default Optional<Hash> getExpectedHead() {
    return Optional.empty();
  }

  /**
   * Mapping of contents-ids to expected global contents-state (think: Iceberg table-metadata),
   * coming from the "expected-state" property of a {@code PutGlobal} commit operation.
   */
  Map<ContentsId, Optional<ByteString>> getExpectedStates();

  /**
   * List of all {@code Put} operations, with their keys, content-types and serialized {@code
   * Contents}.
   */
  List<KeyWithBytes> getPuts();

  /**
   * Mapping of contents-ids to the new contents-state (think: Iceberg table-metadata), coming from
   * the "contents" property of a {@code PutGlobal} commit operation.
   */
  Map<ContentsId, ByteString> getGlobal();

  /** List of "unchanged" keys, from {@code Unchanged} commit operations. */
  List<Key> getUnchanged();

  /** List of "unchanged" keys, from {@code Delete} commit operations. */
  List<Key> getDeletes();

  /** Serialized commit-metadata. */
  ByteString getCommitMetaSerialized();
}

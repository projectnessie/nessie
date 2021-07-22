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
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.WithHash;

/**
 * Tiered-Version-Store Database-adapter interface.
 *
 * <p>All returned {@link Stream}s must be closed.
 */
public interface DatabaseAdapter extends AutoCloseable {

  /** Ensures that mandatory data is present in the repository, does not change an existing repo. */
  void initializeRepo() throws ReferenceConflictException;

  /** Forces a repository to be re-initialized. */
  void reinitializeRepo() throws ReferenceConflictException;

  Stream<Optional<ContentsAndState<ByteString>>> values(
      NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  Stream<CommitLogEntry> commitLog(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException;

  Stream<KeyWithBytes> entries(NamedRef ref, Optional<Hash> hashOnRef, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  Stream<KeyWithType> keys(NamedRef ref, Optional<Hash> hashOnRef, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  /**
   * Commit operation.
   *
   * @param commitAttempt parameters for the commit
   * @return optimistically written commit-log-entry
   */
  Hash commit(CommitAttempt commitAttempt)
      throws ReferenceConflictException, ReferenceNotFoundException;

  Hash transplant(
      BranchName targetBranch,
      Optional<Hash> expectedHash,
      NamedRef source,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException;

  Hash merge(
      NamedRef from, Optional<Hash> fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  Hash toHash(NamedRef ref) throws ReferenceNotFoundException;

  Stream<WithHash<NamedRef>> namedRefs();

  Hash create(NamedRef ref, Optional<NamedRef> target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException;

  void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  void assign(
      NamedRef ref, Optional<Hash> expectedHash, NamedRef assignTo, Optional<Hash> assignToHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  Stream<Diff<ByteString>> diff(
      NamedRef from,
      Optional<Hash> hashOnFrom,
      NamedRef to,
      Optional<Hash> hashOnTo,
      KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  // NOTE: the following is NOT a "proposed" API, just an idea of how the supporting functions
  // for Nessie-GC need to look like.

  /**
   * Retrieve all keys recorded in the global-contents-log.
   *
   * <p>This operation is primarily used by Nessie-GC and must not be exposed via a public API.
   */
  Stream<ContentsIdWithType> globalKeys(ToIntFunction<ByteString> contentsTypeExtractor);

  /**
   * Retrieve all global-contents recorded in the global-contents-log for the given keys +
   * contents-ids. Callers must assume that the result will not be grouped by key or
   * key+contents-id.
   *
   * <p>This operation is primarily used by Nessie-GC and must not be exposed via a public API.
   */
  Stream<ContentsIdAndBytes> globalLog(
      Set<ContentsIdWithType> keys, ToIntFunction<ByteString> contentsTypeExtractor);

  /**
   * Retrieve all distinct contents from all named references for the given key. This operation is
   * used to collect the "reachable" contents during Nessie-GC. For Iceberg, this operation is used
   * to find the "reachable" snapshot-IDs.
   *
   * <p>This operation is primarily used by Nessie-GC and must not be exposed via a public API.
   */
  Stream<KeyWithContentsIdAndBytes> allContents(
      BiFunction<NamedRef, CommitLogEntry, Boolean> continueOnRefPredicate);
}

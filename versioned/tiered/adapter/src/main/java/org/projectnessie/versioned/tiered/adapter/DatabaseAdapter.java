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
package org.projectnessie.versioned.tiered.adapter;

import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentsAndState;
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

  Stream<Optional<ContentsAndState<ByteString, ByteString>>> values(
      NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys) throws ReferenceNotFoundException;

  Stream<CommitLogEntry> commitLog(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException;

  Stream<KeyWithBytes> entries(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException;

  Stream<KeyWithType> keys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException;

  /**
   * Commit operation.
   *
   * @param branch target branch
   * @param expectedHead expected HEAD in {@code branch}
   * @param expectedStates map expected global-states by {@link Key}
   * @param puts serialized put-operations
   * @param unchanged unchanged content-keys
   * @param deletes deleted content-keys
   * @param operationsKeys all keys contained in {@code puts}, {@code unchanged}, {@code deletes}
   * @param commitMetaSerialized serialized commit-metadata
   * @return optimistically written commit-log-entry
   */
  Hash commit(
      BranchName branch,
      Optional<Hash> expectedHead,
      Map<Key, ByteString> expectedStates,
      List<KeyWithBytes> puts,
      Map<Key, ByteString> global,
      List<Key> unchanged,
      List<Key> deletes,
      Set<Key> operationsKeys,
      ByteString commitMetaSerialized)
      throws ReferenceConflictException, ReferenceNotFoundException;

  Hash transplant(
      BranchName targetBranch,
      Optional<Hash> expectedHash,
      NamedRef source,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException;

  Hash merge(
      NamedRef from,
      Optional<Hash> fromHash,
      BranchName toBranch,
      Optional<Hash> expectedHash,
      boolean commonAncestorRequired)
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
      NamedRef from, Optional<Hash> hashOnFrom, NamedRef to, Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException;

  // NOTE: the following is NOT a "proposed" API, just an idea of how the supporting functions
  // for Nessie-GC need to look like.

  /**
   * Retrieve all keys recorded in the global-contents-log, optionally filtered by contents-type.
   *
   * <p>This operation is primarily used by Nessie-GC and must not be exposed via a public API.
   */
  default Stream<KeyWithType> globalKeys(OptionalInt contentType) {
    throw new UnsupportedOperationException("This is a proposal!");
  }

  /**
   * Retrieve all global-contents recorded in the global-contents-log for the given keys, or all
   * keys, if no key-list is present, optionally filtered by contents-type.
   *
   * <p>This operation is primarily used by Nessie-GC and must not be exposed via a public API.
   */
  default Stream<Optional<KeyWithBytes>> globalLog(
      Optional<List<Key>> keys, OptionalInt contentType) {
    throw new UnsupportedOperationException("This is a proposal!");
  }

  /**
   * Retrieve all distinct contents from all named references for the given key. This operation
   * returns the contents is used to collect the "reachable" contents during Nessie-GC. For Iceberg,
   * this operation is used to find the "reachable" snapshot-IDs.
   *
   * <p>This operation is primarily used by Nessie-GC and must not be exposed via a public API.
   *
   * @param key contents-key
   * @param stopCondition condition to evaluate when commit-log traversal shall stop (think:
   *     take-until/exclusive)
   * @param deserializer deserialize to the contents object
   * @param <T> contents-type
   */
  default <T> Stream<T> liveContentsForKey(
      Key key, Predicate<T> stopCondition, Function<ByteString, T> deserializer) {
    throw new UnsupportedOperationException("This is a proposal!");
  }
}

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
 * Database-Adapter interface that encapsulates all database related logic, ab abstraction between a
 * {@link org.projectnessie.versioned.VersionStore} implementation and a variety of different
 * databases that share common core implementations for example for the commit/merge/transplant
 * operations.
 *
 * <p>Database-adapters treat the actual "Nessie contents" and "Nessie commit metadata" as an opaque
 * value ("BLOB") without interpreting the contents. Database-adapter must persist serialized values
 * for commit-metadata and contents as is and must return those in the exact same representation on
 * read.
 *
 * <p>Actual implementation usually extend either {@code
 * org.projectnessie.versioned.persist.nontx.NonTxDatabaseAdapter} (NoSQL databases) or {@code
 * org.projectnessie.versioned.persist.tx.TxDatabaseAdapter} (JDBC/transactional). Both in turn
 * extend {@link org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter}.
 *
 * <p>All returned {@link Stream}s must be closed.
 */
public interface DatabaseAdapter {

  /** Ensures that mandatory data is present in the repository, does not change an existing repo. */
  void initializeRepo(String defaultBranchName);

  /** Forces a repository to be re-initialized. */
  void reinitializeRepo(String defaultBranchName);

  /** Get the {@link Hash} for "beginning of time". */
  Hash noAncestorHash();

  /**
   * Verifies that the given {@code namedReference} exists and that {@code hashOnReference}, if
   * present, is reachable via that reference.
   *
   * @return verified {@code hashOnReference} or, if {@code hashOnReference} is not present, the
   *     current HEAD of {@code namedReference}
   * @throws ReferenceNotFoundException if {@code namedReference} does not exist or {@code
   *     hashOnReference}, if present, is not reachable from that reference
   */
  Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException;

  /**
   * Resolve the current HEAD of the given named-reference.
   *
   * <p>This is actually a convenience for {@link #hashOnReference(NamedRef, Optional)
   * hashOnReference(ref, Optional.empty()}.
   *
   * @param ref named reference to resolve
   * @return current HEAD of {@code ref}
   * @throws ReferenceNotFoundException if the named reference {@code ref} does not exist.
   */
  Hash toHash(NamedRef ref) throws ReferenceNotFoundException;

  /**
   * Retrieve the reference-local and global state for the given keys for the specified commit.
   *
   * @param commit commit to retrieve the values for.
   * @param keys keys to retrieve the values (reference-local and global) for
   * @param keyFilter predicate to optionally skip specific keys in the result and return those as
   *     {@link Optional#empty() "not present"}, for example to implement a security policy.
   * @return Ordered stream
   * @throws ReferenceNotFoundException if {@code commit} does not exist.
   */
  Stream<Optional<ContentsAndState<ByteString>>> values(
      Hash commit, List<Key> keys, KeyFilterPredicate keyFilter) throws ReferenceNotFoundException;

  /**
   * Retrieve the commit-log starting at the commit referenced by {@code offset}.
   *
   * @param offset hash to start at
   * @return stream of {@link CommitLogEntry}s
   * @throws ReferenceNotFoundException if {@code offset} does not exist.
   */
  Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException;

  /**
   * Retrieve the contents-keys that are "present" for the specified commit.
   *
   * @param commit commit to retrieve the values for.
   * @param keyFilter predicate to optionally skip specific keys in the result and return those as
   *     {@link Optional#empty() "not present"}, for example to implement a security policy.
   * @return Ordered stream with content-keys, content-ids and content-types
   * @throws ReferenceNotFoundException if {@code commit} does not exist.
   */
  Stream<KeyWithType> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  /**
   * Commit operation, see {@link CommitAttempt} for a description of the parameters.
   *
   * @param commitAttempt parameters for the commit
   * @return optimistically written commit-log-entry
   * @throws ReferenceNotFoundException if either the named reference in {@link
   *     CommitAttempt#getCommitToBranch()} or the commit on that reference, if specified, does not
   *     exist.
   * @throws ReferenceConflictException if any of the commits could not be committed onto the target
   *     branch due to a conflicting change or if the expected hash in {@link
   *     CommitAttempt#getCommitToBranch()}is not its expected hEAD
   */
  Hash commit(CommitAttempt commitAttempt)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Cherry-pick the commits with the hashes {@code sequenceToTransplant} from named reference
   * {@code source} onto the reference {@code targetBranch}.
   *
   * @param targetBranch named-reference to commit to. If no value for the hash is specified, it
   *     defaults to the named reference's HEAD.
   * @param expectedHead if present, {@code target}'s current HEAD must be equal to this value
   * @param sequenceToTransplant commits in {@code source} to cherry-pick onto {@code targetBranch}
   * @return the hash of the last cherry-picked commit, in other words the new HEAD of the target
   *     branch
   * @throws ReferenceNotFoundException if either the named reference in {@code commitOnReference}
   *     or the commit on that reference, if specified, does not exist.
   * @throws ReferenceConflictException if any of the commits could not be committed onto the target
   *     branch due to a conflicting change or if the expected hash of {@code toBranch} is not its
   *     expected hEAD
   */
  Hash transplant(
      BranchName targetBranch, Optional<Hash> expectedHead, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Merge all commits on {@code from} since the common ancestor of {@code from} and {@code to} and
   * commit those onto {@code to}.
   *
   * <p>The implementation first identifies the common-ancestor (the most-recent commit that is both
   * reachable via {@code from} and {@code to}).
   *
   * @param from commit-hash to start reading commits from.
   * @param toBranch target branch to commit to
   * @param expectedHead if present, {@code toBranch}'s current HEAD must be equal to this value
   * @return the hash of the last cherry-picked commit, in other words the new HEAD of the target
   *     branch
   * @throws ReferenceNotFoundException if either the named reference in {@code toBranch} or the
   *     commit on that reference, if specified, does not exist.
   * @throws ReferenceConflictException if any of the commits could not be committed onto the target
   *     branch due to a conflicting change or if the expected hash of {@code toBranch} is not its
   *     expected hEAD
   */
  Hash merge(Hash from, BranchName toBranch, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Get all named references including their current HEAD.
   *
   * @return stream with all named references including their current HEAD.
   */
  Stream<WithHash<NamedRef>> namedRefs();

  /**
   * Create a new named reference.
   *
   * @param ref Named reference to create - either a {@link org.projectnessie.versioned.BranchName}
   *     or {@link org.projectnessie.versioned.TagName}.
   * @param target The already existing named reference with an optional hash on that branch. This
   *     parameter can be {@code null} for the edge case when the default branch is re-created after
   *     it has been dropped.
   * @return the current HEAD of the created branch or tag
   * @throws ReferenceAlreadyExistsException if the reference {@code ref} already exists.
   * @throws ReferenceNotFoundException if {@code target} does not exist.
   */
  Hash create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException;

  /**
   * Delete the given reference.
   *
   * @param reference named-reference to delete. If a value for the hash is specified, it must be
   *     equal to the current HEAD.
   * @param expectedHead if present, {@code reference}'s current HEAD must be equal to this value
   * @throws ReferenceNotFoundException if the named reference in {@code reference} does not exist.
   * @throws ReferenceConflictException if the named reference's HEAD is not equal to the expected
   *     HEAD
   */
  void delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Updates {@code assignee}'s HEAD to {@code assignTo}.
   *
   * @param assignee named reference to re-assign
   * @param expectedHead if present, {@code assignee}'s current HEAD must be equal to this value
   * @param assignTo commit to update {@code assignee}'s HEAD to
   * @throws ReferenceNotFoundException if either the named reference in {@code assignTo} or the
   *     commit on that reference, if specified, does not exist or if the named reference specified
   *     in {@code assignee} does not exist.
   * @throws ReferenceConflictException if the HEAD of the named reference {@code assignee} is not
   *     equal to the expected HEAD
   */
  void assign(NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Compute the difference of the contents for the two commits identified by {@code from} and
   * {@code to}.
   *
   * @param from {@link Diff#getFromValue() "From"} side of the diff
   * @param to {@link Diff#getToValue() "To" side} of the diff
   * @param keyFilter predicate to optionally skip specific keys in the diff result and not return
   *     those, for example to implement a security policy.
   * @return stream containing the difference of the contents, excluding both equal values and
   *     values that were excluded via {@code keyFilter}
   * @throws ReferenceNotFoundException if {@code from} or {@code to} does not exist.
   */
  Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
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
  Stream<ContentsIdAndBytes> globalContents(
      Set<ContentsId> keys, ToIntFunction<ByteString> contentsTypeExtractor);
}

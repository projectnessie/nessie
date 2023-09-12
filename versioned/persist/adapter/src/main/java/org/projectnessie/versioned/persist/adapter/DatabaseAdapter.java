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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.MustBeClosed;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;

/**
 * Database-Adapter interface that encapsulates all database related logic, an abstraction between a
 * {@link org.projectnessie.versioned.VersionStore} implementation and a variety of different
 * databases that share common core implementations for example for the commit/merge/transplant
 * operations.
 *
 * <p>One or more adapter instances may use the same storage (database instance / schema). In this
 * case adapter instances usually differ by their {@link DatabaseAdapterConfig#getRepositoryId()
 * repository ID} configuration parameters.
 *
 * <p>Database-adapters treat the actual "Nessie content" and "Nessie commit metadata" as an opaque
 * value ("BLOB") without interpreting the content. Database-adapter must persist serialized values
 * for commit-metadata and content as is and must return those in the exact same representation on
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

  DatabaseAdapterConfig getConfig();

  /** Ensures that mandatory data is present in the repository, does not change an existing repo. */
  void initializeRepo(String defaultBranchName);

  /**
   * Forces all repository data managed by this adapter instance to be deleted.
   *
   * <p>This includes all data for the configured {@link DatabaseAdapterConfig#getRepositoryId()
   * repository ID}.
   *
   * <p>After erasing a repository {@link #initializeRepo(String)} may be called to reinitialize the
   * minimal required data structures for the same repository ID.
   */
  void eraseRepo();

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
   * Retrieve the reference-local and global state for the given keys for the specified commit.
   *
   * @param commit commit to retrieve the values for.
   * @param keys keys to retrieve the values (reference-local and global) for
   * @param keyFilter predicate to optionally skip specific keys in the result and return those as
   *     {@link Optional#empty() "not present"}, for example to implement a security policy.
   * @return Ordered stream
   * @throws ReferenceNotFoundException if {@code commit} does not exist.
   */
  Map<ContentKey, ContentAndState> values(
      Hash commit, Collection<ContentKey> keys, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  /**
   * Retrieve the commit-log starting at the commit referenced by {@code offset}.
   *
   * @param offset hash to start at
   * @return stream of {@link CommitLogEntry}s
   * @throws ReferenceNotFoundException if {@code offset} does not exist.
   */
  @MustBeClosed
  Stream<CommitLogEntry> commitLog(Hash offset) throws ReferenceNotFoundException;

  /**
   * Loads commit log entries.
   *
   * @return the loaded {@link CommitLogEntry}s, non-existing entries will not be returned.
   */
  @MustBeClosed
  Stream<CommitLogEntry> fetchCommitLogEntries(Stream<Hash> hashes);

  /**
   * Retrieve the content-keys that are "present" for the specified commit.
   *
   * @param commit commit to retrieve the values for.
   * @param keyFilter predicate to optionally skip specific keys in the result and return those as
   *     {@link Optional#empty() "not present"}, for example to implement a security policy.
   * @return Ordered stream with content-keys, content-ids and content-types
   * @throws ReferenceNotFoundException if {@code commit} does not exist.
   */
  @MustBeClosed
  Stream<KeyListEntry> keys(Hash commit, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  /**
   * Commit operation, see {@link CommitParams} for a description of the parameters.
   *
   * @param commitParams parameters for the commit
   * @return optimistically written commit-log-entry
   * @throws ReferenceNotFoundException if either the named reference in {@link
   *     CommitParams#getToBranch()} or the commit on that reference, if specified, does not exist.
   * @throws ReferenceConflictException if any of the commits could not be committed onto the target
   *     branch due to a conflicting change or if the expected hash in {@link
   *     CommitParams#getToBranch()}is not its expected hEAD
   */
  CommitResult<CommitLogEntry> commit(CommitParams commitParams)
      throws ReferenceConflictException, ReferenceNotFoundException;

  /**
   * Cherry-pick the commits with the hashes {@code sequenceToTransplant} from named reference
   * {@code source} onto the reference {@code targetBranch}.
   *
   * @return the hash of the last cherry-picked commit, in other words the new HEAD of the target
   *     branch
   * @throws ReferenceNotFoundException if either the named reference in {@code commitOnReference}
   *     or the commit on that reference, if specified, does not exist.
   * @throws ReferenceConflictException if any of the commits could not be committed onto the target
   *     branch due to a conflicting change or if the expected hash of {@code toBranch} is not its
   *     expected hEAD
   */
  MergeResult<CommitLogEntry> transplant(TransplantParams transplantParams)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Merge all commits on {@code from} since the common ancestor of {@code from} and {@code to} and
   * commit those onto {@code to}.
   *
   * <p>The implementation first identifies the common-ancestor (the most-recent commit that is both
   * reachable via {@code from} and {@code to}).
   *
   * @return the hash of the last cherry-picked commit, in other words the new HEAD of the target
   *     branch
   * @throws ReferenceNotFoundException if either the named reference in {@code toBranch} or the
   *     commit on that reference, if specified, does not exist.
   * @throws ReferenceConflictException if any of the commits could not be committed onto the target
   *     branch due to a conflicting change or if the expected hash of {@code toBranch} is not its
   *     expected hEAD
   */
  MergeResult<CommitLogEntry> merge(MergeParams mergeParams)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Resolve the current HEAD of the given named-reference and optionally additional information.
   *
   * <p>This is actually a convenience for {@link #hashOnReference(NamedRef, Optional)
   * hashOnReference(ref, Optional.empty()}.
   *
   * @param ref named reference to resolve
   * @param params options that control which information shall be returned in {@link
   *     ReferenceInfo}, see {@link GetNamedRefsParams} for details.
   * @return current HEAD of {@code ref}
   * @throws ReferenceNotFoundException if the named reference {@code ref} does not exist.
   */
  ReferenceInfo<ByteString> namedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException;

  /**
   * Get all named references including their current HEAD.
   *
   * @param params options that control which information shall be returned in each {@link
   *     ReferenceInfo}, see {@link ReferenceInfo} for details.
   * @return stream with all named references.
   */
  @MustBeClosed
  Stream<ReferenceInfo<ByteString>> namedRefs(GetNamedRefsParams params)
      throws ReferenceNotFoundException;

  /**
   * Create a new named reference.
   *
   * @param ref Named reference to create - either a {@link org.projectnessie.versioned.BranchName}
   *     or {@link org.projectnessie.versioned.TagName}.
   * @param target The already existing named reference with an optional hash on that branch. This
   *     parameter can be {@code null} for the edge case when the default branch is re-created after
   *     it has been dropped.
   * @return A {@link ReferenceCreatedResult} containing the head of the created reference
   * @throws ReferenceAlreadyExistsException if the reference {@code ref} already exists.
   * @throws ReferenceNotFoundException if {@code target} does not exist.
   */
  ReferenceCreatedResult create(NamedRef ref, Hash target)
      throws ReferenceAlreadyExistsException, ReferenceNotFoundException;

  /**
   * Delete the given reference.
   *
   * @param reference named-reference to delete. If a value for the hash is specified, it must be
   *     equal to the current HEAD.
   * @param expectedHead if present, {@code reference}'s current HEAD must be equal to this value
   * @return A {@link ReferenceDeletedResult} containing the head of the deleted reference
   * @throws ReferenceNotFoundException if the named reference in {@code reference} does not exist.
   * @throws ReferenceConflictException if the named reference's HEAD is not equal to the expected
   *     HEAD
   */
  ReferenceDeletedResult delete(NamedRef reference, Optional<Hash> expectedHead)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Updates {@code assignee}'s HEAD to {@code assignTo}.
   *
   * @param assignee named reference to re-assign
   * @param expectedHead if present, {@code assignee}'s current HEAD must be equal to this value
   * @param assignTo commit to update {@code assignee}'s HEAD to
   * @return A {@link ReferenceAssignedResult} containing the previous and current head of the
   *     reference
   * @throws ReferenceNotFoundException if either the named reference in {@code assignTo} or the
   *     commit on that reference, if specified, does not exist or if the named reference specified
   *     in {@code assignee} does not exist.
   * @throws ReferenceConflictException if the HEAD of the named reference {@code assignee} is not
   *     equal to the expected HEAD
   */
  ReferenceAssignedResult assign(NamedRef assignee, Optional<Hash> expectedHead, Hash assignTo)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Compute the difference of the content for the two commits identified by {@code from} and {@code
   * to}.
   *
   * @param from {@link Diff#getFromValue() "From"} side of the diff
   * @param to {@link Diff#getToValue() "To" side} of the diff
   * @param keyFilter predicate to optionally skip specific keys in the diff result and not return
   *     those, for example to implement a security policy.
   * @return stream containing the difference of the content, excluding both equal values and values
   *     that were excluded via {@code keyFilter}
   * @throws ReferenceNotFoundException if {@code from} or {@code to} does not exist.
   */
  @MustBeClosed
  Stream<Difference> diff(Hash from, Hash to, KeyFilterPredicate keyFilter)
      throws ReferenceNotFoundException;

  /** Fetches the current version and descriptive attributes of the repository. */
  RepoDescription fetchRepositoryDescription();

  /**
   * Updates the repository description. Takes a function that receives the current repository
   * description and returns the updated description.
   *
   * @param updater updater function, the input argument is never {@code null}, if {@code updater}
   *     return {@code null}, the update will be aborted
   * @throws ReferenceConflictException thrown if the repository description could not be updated
   *     due to other concurrent updates
   */
  void updateRepositoryDescription(Function<RepoDescription, RepoDescription> updater)
      throws ReferenceConflictException;

  /**
   * Retrieves the global content for the given contents-id.
   *
   * @param contentId contents-id to retrieve the global content for
   * @return global content, if present or an empty optional, never {@code null}.
   */
  Optional<ContentIdAndBytes> globalContent(ContentId contentId);

  Map<String, Map<String, String>> repoMaintenance(RepoMaintenanceParams repoMaintenanceParams);

  /**
   * Scan all commit log entries, no guarantees about order nor about the behavior when commits
   * happen while the returned {@link Stream} is consumed.
   */
  @MustBeClosed
  Stream<CommitLogEntry> scanAllCommitLogEntries();

  @VisibleForTesting
  void assertCleanStateForTests();

  /**
   * Write multiple new commit-entries, the given commit entries are to be persisted as is. All
   * values of the given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Callers must call {@link #updateMultipleCommits(List)} for already existing {@link
   * CommitLogEntry}s and {@link #writeMultipleCommits(List)} for new {@link CommitLogEntry}s.
   * Implementations can rely on this assumption (think: SQL {@code INSERT} + {@code UPDATE}
   * compared to a "simple put" for NoSQL databases).
   *
   * <p>Implementations however can enforce strict consistency checks/guarantees, like a best-effort
   * approach to prevent hash-collisions but without any other consistency checks/guarantees.
   */
  void writeMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceConflictException;

  /**
   * Updates multiple commit-entries, the given commit entries are to be persisted as is. All values
   * of the given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Callers must call {@link #updateMultipleCommits(List)} for already existing {@link
   * CommitLogEntry}s and {@link #writeMultipleCommits(List)} for new {@link CommitLogEntry}s.
   * Implementations can rely on this assumption (think: SQL {@code INSERT} + {@code UPDATE}
   * compared to a "simple put" for NoSQL databases).
   *
   * <p>Implementations however <em>can</em> enforce strict consistency checks/guarantees.
   */
  void updateMultipleCommits(List<CommitLogEntry> commitLogEntries)
      throws ReferenceNotFoundException;

  /**
   * Populates the aggregated key-list for the given {@code entry} and returns it.
   *
   * @param entry the {@link CommitLogEntry} to build the aggregated key list for
   * @param inMemoryCommits function to retrieve not-yet-written commit-log-entries
   * @return commit-log-entry with the aggregated key-list. The returned {@link CommitLogEntry} has
   *     not been persisted.
   */
  CommitLogEntry rebuildKeyList(
      CommitLogEntry entry,
      @Nonnull @jakarta.annotation.Nonnull Function<Hash, CommitLogEntry> inMemoryCommits)
      throws ReferenceNotFoundException;
}

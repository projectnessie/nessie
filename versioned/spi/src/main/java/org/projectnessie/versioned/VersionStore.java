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

import static org.projectnessie.versioned.DefaultMetadataRewriter.DEFAULT_METADATA_REWRITER;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.Operation;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.paging.PaginationIterator;

/**
 * A storage interface that maintains multiple versions of the VALUE type with each commit having an
 * associated CommitMeta value.
 */
public interface VersionStore {

  @Nonnull
  RepositoryInformation getRepositoryInformation();

  /**
   * Verifies that the given {@code namedReference} exists and that {@code hashOnReference}, if
   * present, is reachable via that reference.
   *
   * @return verified {@code hashOnReference} or, if {@code hashOnReference} is not present, the
   *     current HEAD of {@code namedReference}
   * @throws ReferenceNotFoundException if {@code namedReference} does not exist or {@code
   *     hashOnReference}, if present, is not reachable from that reference
   */
  Hash hashOnReference(
      NamedRef namedReference,
      Optional<Hash> hashOnReference,
      List<RelativeCommitSpec> relativeLookups)
      throws ReferenceNotFoundException;

  /**
   * Retrieve the hash for "no ancestor" (or "beginning of time"), which is a hash for which no
   * commit exists. "no ancestor" or "beginning of time" are the initial hash of the default branch
   * and branches that are created via {@link #create(NamedRef, Optional)} without specifying the
   * {@code targetHash}.
   *
   * <p>This "no ancestor" value is readable for all users, and it is a valid hash for every named
   * reference.
   *
   * <p>The result of this function must not change for any store instance, but it can be different
   * for different backends and even for different instances of the same backend.
   */
  @Nonnull
  Hash noAncestorHash();

  /**
   * Create a new commit and add to a branch.
   *
   * <p>If {@code referenceHash} is not empty, for each key referenced by one of the operations, the
   * current key's value is compared with the stored value for referenceHash's tree, and {@code
   * ReferenceConflictException} is thrown if values are not matching.
   *
   * @param branch The branch to commit to.
   * @param referenceHash The hash to use as a reference for conflict detection. If not present, do
   *     not perform conflict detection
   * @param metadata The metadata associated with the commit.
   * @param operations The set of operations to apply.
   * @param validator Gets called during the atomic commit operations, callers can implement
   *     validation logic.
   * @param addedContents callback that receives the content-ID of _new_ content per content-key
   * @throws ReferenceConflictException if {@code referenceHash} values do not match the stored
   *     values for {@code branch}
   * @throws ReferenceNotFoundException if {@code branch} is not present in the store
   * @throws NullPointerException if one of the argument is {@code null}
   */
  CommitResult commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull CommitMeta metadata,
      @Nonnull List<Operation> operations,
      @Nonnull CommitValidator validator,
      @Nonnull BiConsumer<ContentKey, String> addedContents)
      throws ReferenceNotFoundException, ReferenceConflictException;

  default CommitResult commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull CommitMeta metadata,
      @Nonnull List<Operation> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return commit(branch, referenceHash, metadata, operations, x -> {}, (k, c) -> {});
  }

  List<RepositoryConfig> getRepositoryConfig(Set<RepositoryConfig.Type> repositoryConfigTypes);

  RepositoryConfig updateRepositoryConfig(RepositoryConfig repositoryConfig)
      throws ReferenceConflictException;

  @FunctionalInterface
  interface CommitValidator {
    void validate(CommitValidation commitValidation)
        throws BaseNessieClientServerException, VersionStoreException;
  }

  @SuppressWarnings("immutables:untype")
  interface MergeTransplantOpBase {
    /** The named ref we are merging/transplanting from. */
    NamedRef fromRef();

    /** The branch that we are merging/transplanting into. */
    BranchName toBranch();

    /** The current head of the branch to validate before updating (optional). */
    @Value.Default
    default Optional<Hash> expectedHash() {
      return Optional.empty();
    }

    /** Function that rewrites the commit metadata. */
    @Value.Default
    default MetadataRewriter<CommitMeta> updateCommitMetadata() {
      return DEFAULT_METADATA_REWRITER;
    }

    /** Merge behaviors per content key. */
    Map<ContentKey, MergeKeyBehavior> mergeKeyBehaviors();

    /** Default merge behavior for all keys not present in {@link #mergeKeyBehaviors()}. */
    @Value.Default
    default MergeBehavior defaultMergeBehavior() {
      return MergeBehavior.NORMAL;
    }

    /** Whether to try the merge, check for conflicts, but do not commit. */
    @Value.Default
    default boolean dryRun() {
      return false;
    }

    /** Whether to fetch additional commit information like commit-operations and parent. */
    @Value.Default
    default boolean fetchAdditionalInfo() {
      return false;
    }

    @Value.Default
    default CommitValidator validator() {
      return commitValidation -> {};
    }
  }

  @Value.Immutable
  interface MergeOp extends MergeTransplantOpBase {

    /** The hash we are using to get additional commits. */
    Hash fromHash();

    static ImmutableMergeOp.Builder builder() {
      return ImmutableMergeOp.builder();
    }
  }

  @Value.Immutable
  interface TransplantOp extends MergeTransplantOpBase {
    /** The sequence of hashes to transplant. */
    List<Hash> sequenceToTransplant();

    static ImmutableTransplantOp.Builder builder() {
      return ImmutableTransplantOp.builder();
    }
  }

  /**
   * Transplant a series of commits to a target branch.
   *
   * <p>This is done as an atomic operation such that only the last of the sequence is ever visible
   * to concurrent readers/writers. The sequence to transplant must be contiguous, in order and
   * share a common ancestor with the target branch.
   *
   * @return merge result
   * @throws ReferenceConflictException if {@code referenceHash} values do not match the stored
   *     values for {@code branch}
   * @throws ReferenceNotFoundException if {@code branch} or if any of the hashes from {@code
   *     sequenceToTransplant} is not present in the store.
   */
  TransplantResult transplant(TransplantOp transplantOp)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Merge items from an existing hash into the requested branch. The merge is always a rebase +
   * fast-forward merge and is only completed if the rebase is conflict free. The set of commits
   * added to the branch will be all of those until we arrive at a common ancestor. Depending on the
   * underlying implementation, the number of commits allowed as part of this operation may be
   * limited
   *
   * <p>Throws if any of the following are true:
   *
   * <ul>
   *   <li>the hash or the branch do not exists
   *   <li>the rebase has conflicts
   *   <li>the expected branch hash does not match the actual branch hash
   * </ul>
   *
   * @return merge result
   * @throws ReferenceConflictException if {@code expectedBranchHash} doesn't match the stored hash
   *     for {@code toBranch}
   * @throws ReferenceNotFoundException if {@code toBranch} or {@code fromHash} is not present in
   *     the store.
   */
  MergeResult merge(MergeOp mergeOp) throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Assign the NamedRef to point to a particular hash.
   *
   * <p>{@code ref} should already exists. If {@code expectedHash} is not empty, its value is
   * compared with the current stored value for {@code ref} and an exception is thrown if values do
   * not match.
   *
   * @param ref The named ref to be assigned
   * @param expectedHash The current head of the NamedRef to validate before updating (required).
   * @param targetHash The hash that this ref should refer to.
   * @return A {@link ReferenceAssignedResult} containing the previous and current head of the
   *     reference
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store or if {@code
   *     targetHash} is not present in the store
   * @throws ReferenceConflictException if {@code expectedHash} is not empty and its value doesn't
   *     match the stored hash for {@code ref}
   */
  ReferenceAssignedResult assign(NamedRef ref, Hash expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Assign the NamedRef to point to a particular hash. If the NamedRef does not exist, it will be
   * created.
   *
   * @param ref The named ref we're assigning
   * @param targetHash The hash that this ref should refer to (optional). Otherwise will reference
   *     the beginning of time.
   * @return A {@link ReferenceCreatedResult} containing the head of the created reference
   * @throws ReferenceNotFoundException if {@code targetHash} is not empty and not present in the
   *     store
   * @throws ReferenceAlreadyExistsException if {@code ref} already exists
   */
  ReferenceCreatedResult create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException;

  /**
   * Delete the provided {@link NamedRef}.
   *
   * <p>Throws exception if the optional hash does not match the provided ref.
   *
   * @param ref The NamedRef to be deleted.
   * @param hash The expected hash (required). The operation will only succeed if the branch is
   *     pointing at the provided hash.
   * @return A {@link ReferenceDeletedResult} containing the head of the deleted reference
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   * @throws ReferenceConflictException if {@code hash} doesn't match the stored hash for {@code
   *     ref}
   */
  ReferenceDeletedResult delete(NamedRef ref, Hash hash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Resolve the given {@link NamedRef} and return information about it, which at least contains the
   * current HEAD commit hash plus, optionally, additional information.
   *
   * <p>This is a functionally equivalent to {@link #hashOnReference(NamedRef, Optional, List)
   * hashOnReference(ref, Optional.empty(), Collections.emptyList())}.
   *
   * @param ref The branch or tag to lookup.
   * @param params options that control which information shall be returned in {@link
   *     ReferenceInfo}, see {@link GetNamedRefsParams} for details.
   * @return Requested information about the requested reference.
   * @throws NullPointerException if {@code ref} is {@code null}.
   * @throws ReferenceNotFoundException if the reference cannot be found
   */
  ReferenceInfo<CommitMeta> getNamedRef(String ref, GetNamedRefsParams params)
      throws ReferenceNotFoundException;

  /**
   * Retrieve the recorded recent history of a reference. A reference's history is a size and time
   * limited record of changes of the reference's current pointer, aka HEAD. The size and time
   * limits are configured in the Nessie server configuration.
   *
   * @return recorded reference history
   */
  ReferenceHistory getReferenceHistory(String refName, Integer headCommitsToScan)
      throws ReferenceNotFoundException;

  /**
   * List named refs.
   *
   * <p><em>IMPORTANT NOTE:</em> The returned {@link Stream} <em>must be closed</em>!
   *
   * @param params options that control which information shall be returned in each {@link
   *     ReferenceInfo}, see {@link ReferenceInfo} for details.
   * @param pagingToken paging token to start at
   * @return All refs and their associated hashes.
   */
  PaginationIterator<ReferenceInfo<CommitMeta>> getNamedRefs(
      GetNamedRefsParams params, String pagingToken) throws ReferenceNotFoundException;

  /**
   * Get a stream of all ancestor commits to a provided ref.
   *
   * @param ref the stream to get commits for.
   * @param fetchAdditionalInfo include additional information like operations and parent hash
   * @return A stream of commits.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  PaginationIterator<Commit> getCommits(Ref ref, boolean fetchAdditionalInfo)
      throws ReferenceNotFoundException;

  @Value.Immutable
  interface KeyRestrictions {
    KeyRestrictions NO_KEY_RESTRICTIONS = KeyRestrictions.builder().build();

    /** Optional, if not {@code null}: the minimum key to return. */
    @Nullable
    ContentKey minKey();

    /** Optional, if not {@code null}: the maximum key to return. */
    @Nullable
    ContentKey maxKey();

    /** Optional, if not {@code null}: the prefix of the keys to return. */
    @Nullable
    ContentKey prefixKey();

    /** Filter predicate, can be {@code null}. */
    @Nullable
    BiPredicate<ContentKey, Content.Type> contentKeyPredicate();

    static ImmutableKeyRestrictions.Builder builder() {
      return ImmutableKeyRestrictions.builder();
    }
  }

  /**
   * Get a stream of all available keys for the given ref.
   *
   * @param ref The ref to get keys for.
   * @param pagingToken paging token to start at
   * @param withContent whether to populate {@link KeyEntry#getContent()}
   * @return The stream of keys available for this ref.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  PaginationIterator<KeyEntry> getKeys(
      Ref ref, String pagingToken, boolean withContent, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException;

  List<IdentifiedContentKey> getIdentifiedKeys(Ref ref, Collection<ContentKey> keys)
      throws ReferenceNotFoundException;

  /**
   * Get the value for a provided ref.
   *
   * @param ref Any ref type allowed
   * @param key The key for the specific value
   * @param returnNotFound whether to return a non-{@code null} result object with a {@code null}
   *     value in {@link ContentResult#content()} instead of a {@code null} return-value for
   *     non-existing content
   * @return The value or {@code null} if the key does not exist and {@code returnNotFound} is
   *     {@code false}. If the content does not exist and {@code returnNotFound} is {@code true},
   *     {@link ContentResult#content()} will be {@code null}.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  ContentResult getValue(Ref ref, ContentKey key, boolean returnNotFound)
      throws ReferenceNotFoundException;

  /**
   * Get the values for a list of keys.
   *
   * @param ref The ref to use.
   * @param keys An ordered list of keys to retrieve within the provided ref.
   * @param returnNotFound whether to return non-{@code null} values with a {@code null} value in
   *     {@link ContentResult#content()} instead of omitting non-existing content in the returned
   *     map
   * @return Map of keys to content results. For keys that do not exist: if {@code returnNotFound}
   *     is {@code false}, the map will not contain an entry for non-existing keys.If {@code
   *     returnNotFound} is {@code true}, the map will return entries for all requested keys, but
   *     {@link ContentResult#content()} will be {@code null} for non-existing keys.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  Map<ContentKey, ContentResult> getValues(
      Ref ref, Collection<ContentKey> keys, boolean returnNotFound)
      throws ReferenceNotFoundException;

  /**
   * Get list of diffs between two refs.
   *
   * @param from The from part of the diff.
   * @param to The to part of the diff.
   * @param pagingToken paging token to start at
   * @return A stream of values that are different.
   */
  PaginationIterator<Diff> getDiffs(
      Ref from, Ref to, String pagingToken, KeyRestrictions keyRestrictions)
      throws ReferenceNotFoundException;
}

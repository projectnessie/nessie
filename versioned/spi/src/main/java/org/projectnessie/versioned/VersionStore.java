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
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A storage interface that maintains multiple versions of the VALUE type with each commit having an
 * associated METADATA value.
 *
 * @param <VALUE> The type of data that will be associated with each key. Values must provide an
 *     associated Serializer.
 * @param <METADATA> The type of data that will be associated with each commit. Metadata values must
 *     provide an associated Serializer.
 */
@ThreadSafe
public interface VersionStore<VALUE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>> {

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
   * Provide the current hash for the given NamedRef.
   *
   * <p>This is a functionally equivalent to {@link #hashOnReference(NamedRef, Optional)
   * hashOnReference(ref, Optional.empty())}.
   *
   * @param ref The Branch or Tag to lookup.
   * @return The current hash for that ref.
   * @throws NullPointerException if {@code ref} is {@code null}.
   * @throws ReferenceNotFoundException if the reference cannot be found
   */
  @Nonnull
  Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException;

  /**
   * Determine what kind of ref a string is and convert it into the appropriate type (along with the
   * current associated hash).
   *
   * <p>If a branch or tag has the same name as the string form of a valid hash, the branch or tag
   * name are returned.
   *
   * @param refOfUnknownType A string that may be a branch, tag or hash.
   * @return The concrete ref type with its hash.
   * @throws ReferenceNotFoundException If the string doesn't map to a valid ref.
   * @throws NullPointerException if {@code refOfUnknownType} is {@code null}.
   */
  WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException;

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
   * @throws ReferenceConflictException if {@code referenceHash} values do not match the stored
   *     values for {@code branch}
   * @throws ReferenceNotFoundException if {@code branch} is not present in the store
   * @throws NullPointerException if one of the argument is {@code null}
   */
  Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Transplant a series of commits to a target branch.
   *
   * <p>This is done as an atomic operation such that only the last of the sequence is ever visible
   * to concurrent readers/writers. The sequence to transplant must be contiguous, in order and
   * share a common ancestor with the target branch.
   *
   * @param targetBranch The branch we're transplanting to
   * @param referenceHash The hash to use as a reference for conflict detection. If not present, do
   *     not perform conflict detection
   * @param sequenceToTransplant The sequence of hashes to transplant.
   * @throws ReferenceConflictException if {@code referenceHash} values do not match the stored
   *     values for {@code branch}
   * @throws ReferenceNotFoundException if {@code branch} or if any of the hashes from {@code
   *     sequenceToTransplant} is not present in the store.
   */
  void transplant(
      BranchName targetBranch, Optional<Hash> referenceHash, List<Hash> sequenceToTransplant)
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
   * @param fromHash The hash we are using to get additional commits
   * @param toBranch The branch that we are merging into
   * @param expectedHash The current head of the branch to validate before updating (optional).
   * @throws ReferenceConflictException if {@code expectedBranchHash} doesn't match the stored hash
   *     for {@code toBranch}
   * @throws ReferenceNotFoundException if {@code toBranch} or {@code fromHash} is not present in
   *     the store.
   */
  void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Assign the NamedRef to point to a particular hash.
   *
   * <p>{@code ref} should already exists. If {@code expectedHash} is not empty, its value is
   * compared with the current stored value for {@code ref} and an exception is thrown if values do
   * not match.
   *
   * @param ref The named ref to be assigned
   * @param expectedHash The current head of the NamedRef to validate before updating. If not
   *     present, force assignment.
   * @param targetHash The hash that this ref should refer to.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store or if {@code
   *     targetHash} is not present in the store
   * @throws ReferenceConflictException if {@code expectedHash} is not empty and its value doesn't
   *     match the stored hash for {@code ref}
   */
  void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Assign the NamedRef to point to a particular hash. If the NamedRef does not exist, it will be
   * created.
   *
   * @param ref The named ref we're assigning
   * @param targetHash The hash that this ref should refer to (optional). Otherwise will reference
   *     the beginning of time.
   * @throws ReferenceNotFoundException if {@code targetHash} is not empty and not present in the
   *     store
   * @throws ReferenceAlreadyExistsException if {@code ref} already exists
   */
  Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException;

  /**
   * Delete the provided NamedRef
   *
   * <p>Throws exception if the optional hash does not match the provided ref.
   *
   * @param ref The NamedRef to be deleted.
   * @param hash An optional hash. If provided, this operation will only succeed if the branch is
   *     pointing at the provided
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   * @throws ReferenceConflictException if {@code hash} doesn't match the stored hash for {@code
   *     ref}
   */
  void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * List named refs.
   *
   * <p><em>IMPORTANT NOTE:</em> The returned {@link Stream} <em>must be closed</em>!
   *
   * @return All refs and their associated hashes.
   */
  Stream<WithHash<NamedRef>> getNamedRefs();

  /**
   * Get a stream of all ancestor commits to a provided ref.
   *
   * @param ref the stream to get commits for.
   * @return A stream of commits.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException;

  /**
   * Get a stream of all available keys for the given ref.
   *
   * @param ref The ref to get keys for.
   * @return The stream of keys available for this ref.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  Stream<WithType<Key, VALUE_TYPE>> getKeys(Ref ref) throws ReferenceNotFoundException;

  /**
   * Get the value for a provided ref.
   *
   * @param ref Any ref type allowed
   * @param key The key for the specific value
   * @return The value.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException;

  /**
   * Get the values for a list of keys.
   *
   * @param ref The ref to use.
   * @param keys An ordered list of keys to retrieve within the provided ref.
   * @return A parallel list of values.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  List<Optional<VALUE>> getValues(Ref ref, List<Key> keys) throws ReferenceNotFoundException;

  /**
   * Get list of diffs between two refs.
   *
   * @param from The from part of the diff.
   * @param to The to part of the diff.
   * @return A stream of values that are different.
   */
  Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException;
}

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
package com.dremio.nessie.versioned;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

/**
 * A storage interface that maintains multiple versions of the VALUE type with each commit having an associated
 * METADATA value.
 *
 * @param <VALUE>    The type of data that will be associated with each key. Values must provide an associated
 *                   Serializer.
 * @param <METADATA> The type of data that will be associated with each commit. Metadata values must provide an
 *                   associated Serializer.
 */
public interface VersionStore<VALUE, METADATA> {

  /**
   * Provide the current hash for the given NamedRef.
   *
   * @param ref The Branch or Tag to lookup.
   * @return The current hash for that ref.
   * @throws NullPointerException if {@code ref} is {@code null}.
   * @throws ReferenceNotFoundException if the reference cannot be found
   */
  @Nonnull
  Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException;

  /**
   * Create a new commit and add to a branch.
   *
   * @param branch The branch to commit to.
   * @param expectedHash The current head of the branch to validate before updating (optional).
   * @param metadata The metadata associated with the commit.
   * @param operations The set of operations to apply.
   * @throws ReferenceConflictException if {@code currentBranchHash} doesn't match the stored hash for {@code branch}
   * @throws ReferenceNotFoundException if {@code branch} is not present in the store
   *
   */
  void commit(BranchName branch, Optional<Hash> expectedHash, METADATA metadata, List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Transplant a series of commits to a target branch. This is done as an atomic operation such that only the last of
   * the sequence is ever visible to concurrent readers/writers. The sequence to transplant must be contiguous, in order
   * and share a common ancestor to the target branch.
   *
   * @param targetBranch         The branch we're transplanting to
   * @param expectedHash         The current head of the branch to validate before updating (optional).
   * @param sequenceToTransplant The sequence of hashes to transplant.
   * @throws ReferenceConflictException if {@code currentBranchHash} doesn't match the stored hash for {@code branch}
   * @throws ReferenceNotFoundException if {@code branch} or if any of the hashes from {@code sequenceToTransplant} is not present in the
   *     store.
   */
  void transplant(BranchName targetBranch, Optional<Hash> expectedHash, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Merge items from an existing hash into the requested branch. The merge is always a rebase + fast-forward merge and
   * is only completed if the rebase is conflict free. The set of commits added to the branch will be all of those until
   * we arrive at a common ancestor. Depending on the underlying implementation, the number of commits allowed as part
   * of this operation may be limited
   *
   * <p>Throws if any of the following are true:
   *
   * <ul>
   * <li>the hash or the branch do not exists
   * <li>the rebase has conflicts
   * <li>the expected branch hash does not match the actual branch hash
   * </ul>
   *
   * @param fromHash     The hash we are using to get additional commits
   * @param toBranch     The branch that we are merging into
   * @param expectedHash The current head of the branch to validate before updating (optional).
   * @throws ReferenceConflictException if {@code expectedBranchHash} doesn't match the stored hash for {@code toBranch}
   * @throws ReferenceNotFoundException if {@code toBranch} or {@code fromHash} is not present in the store.
   */
  void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Assign the NamedRef to point to a particular hash. If the NamedRef does not exist, it will be created.
   *
   * <p>Throws if currentBranchHash is defined and does not match the current hash of the provided branch
   *
   * <p>Throws if the targetHash provided does not exist, an exception will be thrown.
   *
   * @param ref          The named ref we're assigning
   * @param expectedHash The current head of the NamedRef to validate before updating (optional).
   * @param targetHash   The hash that this ref should refer to.
   * @throws ReferenceNotFoundException if {@code targetHash} is not present in the store, or {@code currentBranchHash} is not empty
   *     and {@code ref} is not present in the store
   * @throws ReferenceConflictException if {@code currentBranchHash} doesn't match the stored hash for {@code ref}
   */
  void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * Assign the NamedRef to point to a particular hash. If the NamedRef does not exist, it will be created.
   *
   * @param ref        The named ref we're assigning
   * @param targetHash The hash that this ref should refer to (optional). Otherwise will reference the beginning of time.
   * @throws ReferenceNotFoundException if {@code targetHash} is not empty and not present in the store
   * @throws ReferenceAlreadyExistsException if {@code ref} already exists
   */
  void create(NamedRef ref, Optional<Hash> targetHash) throws ReferenceNotFoundException, ReferenceAlreadyExistsException;

  /**
   * Delete the provided NamedRef
   *
   * <p>Throws exception if the optional hash does not match the provided ref.
   *
   * @param ref The NamedRef to be deleted.
   * @param hash An optional hash. If provided, this operation will only succeed if the branch is pointing at the provided
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   * @throws ReferenceConflictException if {@code hash} doesn't match the stored hash for {@code ref}
   */
  void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException;

  /**
   * List named refs.
   * @return All refs and their associated hashes.
   */
  Stream<WithHash<NamedRef>> getNamedRefs();

  /**
   * Get a stream of all ancestor commits to a provided ref.
   * @param ref the stream to get commits for.
   * @return A stream of commits.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException;

  /**
   * Get a stream of all available keys for the given ref.
   * @param ref The ref to get keys for.
   * @return The stream of keys available for this ref.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException;

  /**
   * Get the value for a provided ref.
   * @param ref Any ref type allowed
   * @param key The key for the specific value
   * @return The value.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException;

  /**
   * Get the values for a list of keys.
   * @param ref The ref to use.
   * @param key An ordered list of keys to retrieve within the provided ref.
   * @return A parallel list of values.
   * @throws ReferenceNotFoundException if {@code ref} is not present in the store
   */
  List<Optional<VALUE>> getValue(Ref ref, List<Key> key) throws ReferenceNotFoundException;

  /**
   * Collect some garbage. Each time this is called, it collects some garbage and reports the progress of what has been collected
   * @return A collector object that must be closed if not depleted.
   */
  Collector collectGarbage();

  /**
   * A garbage collector that can be used to collect metadata.
   */
  public interface Collector extends AutoCloseable, Iterator<CollectionProgress> {}

  /**
   * The current progress of collection.
   */
  public interface CollectionProgress {
  }

}

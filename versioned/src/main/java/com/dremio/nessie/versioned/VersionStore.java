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
   * <p>Throws if the provided NamedRef does not exist.
   *
   * @param ref The Branch or Tag to lookup.
   * @return The current hash for that ref.
   */
  Hash toHash(NamedRef ref);

  /**
   * Create a new commit to a branch.
   *
   * <p>Throws if branch does not exist.
   *
   * <p>Throws if operation failed due to any of the Operation preconditions expected.
   *
   * @param branch The branch to commit to.
   * @param currentBranchHash The current expected head of the branch.
   * @param metadata The metadata associated with the commit.
   * @param operations The set of operations to apply.
   *
   */
  void commit(BranchName branch, Optional<Hash> currentBranchHash, METADATA metadata, List<Operation<VALUE>> operations);

  /**
   * Transplant a series of commits to a target branch. This is done as an atomic operation such that only the last of
   * the sequence is ever visible to concurrent readers/writers.
   *
   * @param branch               The branch
   * @param currentBranchHash    The required current version of the branch (if provided)
   * @param sequenceToTransplant The ordered list of hashes to transplant.
   */
  void transplant(BranchName branch, Optional<Hash> currentBranchHash, List<Hash> sequenceToTransplant);

  /**
   * Assign the NamedRef to point to a particular hash. If the NamedRef does not exist, it will be created.
   *
   * <p>Throws if currentBranchHash is defined and does not match the current hash of the provided branch
   *
   * <p>Throws if the targetHash provided does not exist, an exception will be thrown.
   *
   * @param ref               The named ref we're assigning
   * @param currentBranchHash The current hash the ref is pointing to.
   * @param targetHash         The hash that this ref should refer to.
   */
  void assign(NamedRef ref, Optional<Hash> currentBranchHash, Hash targetHash);

  /**
   * Delete the provided NamedRef
   *
   * <p>Throws exception if the optional hash does not match the provided ref.
   *
   * @param ref The NamedRef to be deleted.
   * @param hash An optional hash. If provided, this operation will only succeed if the branch is pointing at the provided
   */
  void delete(NamedRef ref, Optional<Hash> hash);

  /**
   * List named refs.
   * @return All refs and their associated hashes.
   */
  Stream<WithHash<NamedRef>> getNamedRefs();

  /**
   * Get a stream of all ancestor commits to a provided ref.
   * @param ref the stream to get commits for.
   * @return A stream of commits.
   */
  Stream<WithHash<METADATA>> getCommits(Ref ref);

  /**
   * Get a stream of all available keys for the given ref.
   * @param ref The ref to get keys for.
   * @return The stream of keys available for this ref.
   */
  Stream<Key> getKeys(Ref ref);

  /**
   * Get the value for a provided ref.
   * @param ref Any ref type allowed
   * @param key The key for the specific value
   * @return The value. Throws if ref or key within ref does not exist.
   */
  VALUE getValue(Ref ref, Key key);

  /**
   * Get the values for a list of keys.
   * @param ref The ref to use.
   * @param key An ordered list of keys to retrieve within the provided ref.
   * @return A parallel list of values.
   */
  List<Optional<VALUE>> getValue(Ref ref, List<Key> key);

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

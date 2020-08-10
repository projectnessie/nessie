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
 * A storage interface that maintains multiple versions of the value type.
 *
 * @param <VALUE> The value type the version store stores.
 */
public interface VersionStore<VALUE, COMMIT> {

  /**
   * Provide the current hash for the given ref.
   *
   * Throws if the ref does not exists.
   * @param ref The Branch or Tag to lookup.
   * @return
   */
  Hash toHash(Ref ref);

  /**
   * Create a new commit to a branch.
   * @param branch The branch to commit to.
   * @param expectedHash The current expected head of the branch.
   * @param metadata The metadata associated with the commit.
   * @param operations The set of operations to apply.
   *
   * Throws if branch does not exist.
   * Throws if operation failed due to any of the Operation preconditions expected.
   */
  void commit(BranchName branch, Optional<Hash> currentBranchHash, COMMIT commit, List<Operation<VALUE>> operations);

  /**
   * List the current branches that exist.
   * @return
   */
  Stream<WithHash<BranchName>> getBranches();

  /**
   * Assign a branch to point to a particular hash. If the branch does not exist, it will be created.
   *
   * Throws if currentBranchHash is defined and does not match the current hash of the provided branch
   * Throws if the targetHash provided does not exist, an exception will be thrown.
   *
   * @param branch            The branch we're assigning
   * @param tagetHash              The hash that this branch should refer to.
   * @param currentBranchHash The current hash the branch is pointing to.
   */
  void assignBranch(BranchName branch, Hash targetHash, Optional<Hash> currentBranchHash);

  /**
   * Delete the provided ref. Ref must be a tag or branch.
   *
   * Throws exception if the optional hash does not match the provided branch. Also throws exception if the provided ref is a hash as hashes are not deletable
   *
   * @param branch The branch to be deleted.
   * @param hash An optional hash. If provided, this operation will only succeed if the branch is pointing at the provided
   */
  void delete(Ref ref, Optional<Hash> hash);

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
   * List the available tags.
   * @return
   */
  Stream<WithHash<TagName>> getTags();

  /**
   * Assign a tag to the provided targetHash. If the tag does not exist, create it.
   *
   * Throws if the targetHash is not defined or the tag is not pointing at currentTagHash (if provided).
   *
   * @param tag The tag to assign and/or create.
   * @param targetHash The hash to point the tag at.
   * @param currentTagHash The current expected hash of the provided tag.
   */
  void assignTag(TagName tag, Hash hash, Optional<Hash> currentTagHash);

  /**
   * Get a stream of all ancestor commits to a provided ref.
   * @param ref
   * @return
   */
  Stream<WithHash<COMMIT>> getCommits(Ref ref);

  /**
   * Get a stream of all available keys for the given ref.
   * @param ref
   * @return
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
   * @param The ref to use.
   * @param An ordered list of keys to retrieve within the provided ref.
   * @return A parallel list of values.
   */
  List<Optional<VALUE>> getValue(Ref ref, List<Key> key);

  // TODO: deal with moves (how?)

  /**
   * Collect some garbage. Each time this is called, it collects some garbage and reports the progress of what has been collected
   * @return A collector object that must be closed if not depleted.
   */
  Collector collectGarbage();

  public interface Collector extends AutoCloseable, Iterator<CollectionProgress> {}
  public interface CollectionProgress {}
}

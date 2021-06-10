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
package org.projectnessie.versioned.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;

/** Helper to generate commits against a store. */
public class CommitBuilder<ValueT, MetadataT, EnumT extends Enum<EnumT>> {
  private final List<Operation<ValueT>> operations = new ArrayList<>();
  private final VersionStore<ValueT, MetadataT, EnumT> store;
  private MetadataT metadata = null;
  private Optional<Hash> referenceHash = Optional.empty();
  private boolean fromLatest = false;

  public CommitBuilder(VersionStore<ValueT, MetadataT, EnumT> store) {
    this.store = store;
  }

  /**
   * Adds a put operation to the current commit.
   *
   * @param key key's name to add
   * @param value the value associated with the key
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> put(String key, ValueT value) {
    return put(Key.of(key), value);
  }

  /**
   * Adds a put operation to the current commit.
   *
   * @param key key to add
   * @param value the value associated with the key
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> put(Key key, ValueT value) {
    return add(Put.of(key, value));
  }

  /**
   * Adds a delete operation to the current commit.
   *
   * @param key key's name to delete
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> delete(String key) {
    return delete(Key.of(key));
  }

  /**
   * Adds a delete operation to the current commit.
   *
   * @param key key to delete
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> delete(Key key) {
    return add(Delete.of(key));
  }

  /**
   * Adds an unchanged operation to the current commit.
   *
   * @param key key's name for the operation
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> unchanged(String key) {
    return unchanged(Key.of(key));
  }

  /**
   * Adds an unchanged operation to the current commit.
   *
   * @param key key for the operation
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> unchanged(Key key) {
    return add(Unchanged.of(key));
  }

  /**
   * Adds an operation to the current commit.
   *
   * @param operation operation to commit
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> add(Operation<ValueT> operation) {
    operations.add(operation);
    return this;
  }

  /**
   * Sets metadata for the current commit.
   *
   * @param metadata metadata to associate with the commit
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> withMetadata(MetadataT metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Sets a reference to base the commit on.
   *
   * @param reference the reference's hash
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> fromReference(Hash reference) {
    referenceHash = Optional.of(reference);
    fromLatest = false;
    return this;
  }

  /**
   * Uses the latest remote hash as a reference to base the commit on.
   *
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT, EnumT> fromLatest() {
    fromLatest = true;
    return this;
  }

  /**
   * Pushes the commit to the branch.
   *
   * @param branchName the branch name
   * @return the hash associated with the commit
   */
  public Hash toBranch(String branchName)
      throws ReferenceNotFoundException, ReferenceConflictException {
    return toBranch(BranchName.of(branchName));
  }

  /**
   * Pushes the commit to the branch.
   *
   * @param branchName the branch
   * @return the hash associated with the commit
   */
  public Hash toBranch(BranchName branchName)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Optional<Hash> reference = fromLatest ? Optional.of(store.toHash(branchName)) : referenceHash;
    Hash commitHash = store.commit(branchName, reference, metadata, operations);
    Hash storeHash = store.toHash(branchName);
    Assertions.assertEquals(storeHash, commitHash);
    return commitHash;
  }
}

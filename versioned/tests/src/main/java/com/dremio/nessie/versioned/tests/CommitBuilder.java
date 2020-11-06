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
package com.dremio.nessie.versioned.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;

/**
 * Helper to generate commits against a store.
 */
public class CommitBuilder<ValueT, MetadataT> {
  private final List<Operation<ValueT>> operations = new ArrayList<>();
  private final VersionStore<ValueT, MetadataT> store;
  private MetadataT metadata = null;
  private Optional<Hash> referenceHash = Optional.empty();
  private boolean fromLatest = false;

  public CommitBuilder(VersionStore<ValueT, MetadataT> store) {
    this.store = store;
  }

  /**
   * Adds a put operation to the current commit.
   * @param key key's name to add
   * @param value the value associated with the key
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> put(String key, ValueT value) {
    return put(Key.of(key), value);
  }

  /**
   * Adds a put operation to the current commit.
   * @param key key to add
   * @param value the value associated with the key
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> put(Key key, ValueT value) {
    return add(Put.of(key, value));
  }

  /**
   * Adds a delete operation to the current commit.
   * @param key key's name to delete
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> delete(String key) {
    return delete(Key.of(key));
  }

  /**
   * Adds a delete operation to the current commit.
   * @param key key to delete
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> delete(Key key) {
    return add(Delete.of(key));
  }

  /**
   * Adds an unchanged operation to the current commit.
   * @param key key's name for the operation
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> unchanged(String key) {
    return unchanged(Key.of(key));
  }

  /**
   * Adds an unchanged operation to the current commit.
   * @param key key for the operation
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> unchanged(Key key) {
    return add(Unchanged.of(key));
  }

  /**
   * Adds an operation to the current commit.
   * @param operation operation to commit
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> add(Operation<ValueT> operation) {
    operations.add(operation);
    return this;
  }

  /**
   * Sets metadata for the current commit.
   * @param metadata metadata to associate with the commit
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> withMetadata(MetadataT metadata) {
    this.metadata = metadata;
    return this;
  }

  /**
   * Sets a reference to base the commit on.
   * @param reference the reference's hash
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> fromReference(Hash reference) {
    referenceHash = Optional.of(reference);
    fromLatest = false;
    return this;
  }

  /**
   * Uses the latest remote hash as a reference to base the commit on.
   * @return the builder instance
   */
  public CommitBuilder<ValueT, MetadataT> fromLatest() {
    fromLatest = true;
    return this;
  }

  /**
   * Pushes the commit to the branch.
   * @param branchName the branch name
   * @return the hash associated with the commit
   */
  Hash toBranch(String branchName) throws ReferenceNotFoundException, ReferenceConflictException {
    return toBranch(BranchName.of(branchName));
  }

  /**
   * Pushes the commit to the branch.
   * @param branchName the branch
   * @return the hash associated with the commit
   */
  Hash toBranch(BranchName branchName) throws ReferenceNotFoundException, ReferenceConflictException {
    Optional<Hash> reference = fromLatest ? Optional.of(store.toHash(branchName)) : referenceHash;
    store.commit(branchName, reference, metadata, operations);
    return store.toHash(branchName);
  }
}

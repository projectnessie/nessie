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
package org.projectnessie.versioned.memory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.Unchanged;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Commit class.
 *
 * @param <ValueT> commit value
 * @param <MetadataT> commit metadata
 */
class Commit<ValueT, MetadataT> {
  private static final HashFunction COMMIT_HASH_FUNCTION = Hashing.sha256();

  public static final Hash NO_ANCESTOR = Hash.of(
      UnsafeByteOperations.unsafeWrap(COMMIT_HASH_FUNCTION.newHasher().putString("empty", StandardCharsets.UTF_8).hash().asBytes()));

  private final Hash hash;
  private final Hash ancestor;
  private final MetadataT metadata;
  private final List<Operation<ValueT>> operations;

  public Commit(Hash hash, Hash ancestor, MetadataT metadata, List<Operation<ValueT>> operations) {
    this.hash = requireNonNull(hash);
    this.ancestor = requireNonNull(ancestor);
    this.metadata = requireNonNull(metadata);
    this.operations = ImmutableList.copyOf(requireNonNull(operations));
  }

  public static <ValueT, MetadataT> Commit<ValueT, MetadataT> of(final Serializer<ValueT> valueSerializer,
      final Serializer<MetadataT> metadataSerializer, Hash ancestor, MetadataT metadata,
      List<Operation<ValueT>> operations) {
    // Create a hash for the commit
    Hasher hasher = COMMIT_HASH_FUNCTION.newHasher();

    // Previous commit
    hasher.putString("ancestor", UTF_8);
    hash(hasher, ancestor.asBytes());

    // serialize metadata and hash
    hasher.putString("metadata", UTF_8);
    hash(hasher, metadataSerializer.toBytes(metadata));

    // serialize operations and hash
    for (Operation<ValueT> operation: operations) {
      if (operation instanceof Put) {
        Put<ValueT> put = (Put<ValueT>) operation;
        hasher.putString("put", UTF_8);
        hash(hasher, put.getKey());
        hash(hasher, valueSerializer.toBytes(put.getValue()));
      } else if (operation instanceof Delete) {
        Delete<ValueT> delete = (Delete<ValueT>) operation;
        hasher.putString("delete", UTF_8);
        hash(hasher, delete.getKey());
      } else if (operation instanceof Unchanged) {
        Unchanged<ValueT> unchanged = (Unchanged<ValueT>) operation;
        hash(hasher, unchanged.getKey());
        unchanged.getKey().getElements().forEach(e -> hasher.putString(e, UTF_8));
      } else {
        throw new IllegalArgumentException("Unknown operation type for operation " + operation);
      }
    }

    final Hash commitHash = Hash.of(UnsafeByteOperations.unsafeWrap(hasher.hash().asBytes()));
    return new Commit<>(commitHash, ancestor, metadata, operations);
  }

  private static final Hasher hash(Hasher hasher, ByteString bytes) {
    bytes.asReadOnlyByteBufferList().forEach(hasher::putBytes);
    return hasher;
  }

  private static final Hasher hash(Hasher hasher, Key key) {
    key.getElements().forEach(e -> hasher.putString(e.toLowerCase(Locale.ROOT), UTF_8));
    return hasher;
  }

  public Hash getHash() {
    return hash;
  }

  public Hash getAncestor() {
    return ancestor;
  }

  public MetadataT getMetadata() {
    return metadata;
  }

  public List<Operation<ValueT>> getOperations() {
    return operations;
  }

  @Override public String toString() {
    return "Commit [hash=" + hash + ", ancestor=" + ancestor + ", metadata=" + metadata
        + ", operations=" + operations + "]";
  }


}

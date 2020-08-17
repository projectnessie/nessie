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
package com.dremio.nessie.versioned.memory;

import static com.dremio.nessie.versioned.memory.Commit.NO_ANCESTOR;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.versioned.BranchName;
import com.dremio.nessie.versioned.Delete;
import com.dremio.nessie.versioned.Hash;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.NamedRef;
import com.dremio.nessie.versioned.Operation;
import com.dremio.nessie.versioned.Put;
import com.dremio.nessie.versioned.Ref;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.Unchanged;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.WithHash;
import com.google.common.base.Throwables;
import com.google.common.collect.Streams;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * In-memory implementation of {@code VersionStore} interface.
 *
 * @param <ValueT> Value type
 * @param <MetadataT> Commit metadata type
 */
public class InMemoryVersionStore<ValueT, MetadataT> implements VersionStore<ValueT, MetadataT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryVersionStore.class);

  private static final HashFunction COMMIT_HASH_FUNCTION = Hashing.sha256();

  private final ConcurrentMap<Hash, Commit<ValueT, MetadataT>> commits = new ConcurrentHashMap<>();
  private final ConcurrentMap<NamedRef, Hash> namedReferences = new ConcurrentHashMap<>();
  private final Serializer<ValueT> valueSerializer = null;
  private final Serializer<MetadataT> metadataSerializer = null;


  @Override
  public Hash toHash(NamedRef ref) throws ReferenceNotFoundException {
    final Hash hash = namedReferences.get(requireNonNull(ref));
    if (hash == null) {
      throw ReferenceNotFoundException.forReference(ref);
    }
    return hash;
  }

  private Hash toHash(Ref ref) throws ReferenceNotFoundException {
    if (ref instanceof NamedRef) {
      return toHash((NamedRef) ref);
    } else if (ref instanceof Hash) {
      return (Hash) ref;
    } else {
      throw new IllegalArgumentException(format("Unsupported reference type for ref %s", ref));
    }
  }

  @Override
  public void commit(BranchName branch, Optional<Hash> expectedBranchHash,
      MetadataT metadata, List<Operation<ValueT>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    final Hash currentHash = checkExpectedHash(branch, expectedBranchHash);

    // Create a hash for the commit
    Hasher hasher = COMMIT_HASH_FUNCTION.newHasher();

    // Previous commit
    hasher.putString("ancestor", UTF_8);
    hash(hasher, currentHash.asBytes());

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
    final Commit<ValueT, MetadataT> commit = new Commit<>(expectedBranchHash.orElse(null), metadata, operations);

    // Storing
    tryCompute(namedReferences, branch, (key, hash) -> {
      final Hash previousHash = Optional.ofNullable(hash).orElse(NO_ANCESTOR);
      if (!previousHash.equals(currentHash)) {
        throw ReferenceConflictException.forReference(branch, expectedBranchHash, Optional.of(previousHash));
      }

      commits.putIfAbsent(commitHash, commit);
      return commitHash;
    });
  }

  private static final Hasher hash(Hasher hasher, ByteString bytes) {
    bytes.asReadOnlyByteBufferList().forEach(hasher::putBytes);
    return hasher;
  }

  private static final Hasher hash(Hasher hasher, Key key) {
    key.getElements().forEach(e -> hasher.putString(e, UTF_8));
    return hasher;
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> expectedBranchHash,
      List<Hash> sequenceToTransplant) throws ReferenceNotFoundException, ReferenceConflictException {
    requireNonNull(targetBranch);
    requireNonNull(sequenceToTransplant);

    final Hash expectedHash = checkExpectedHash(targetBranch, expectedBranchHash);

    if (sequenceToTransplant.isEmpty()) {
      return;
    }

    // check that all hashes exist in the store
    Hash previousHash = null;
    boolean foundExpectedHash = false;
    for (Hash hash: sequenceToTransplant) {
      final Commit<ValueT, MetadataT> commit = commits.get(hash);
      if (commit == null) {
        throw ReferenceNotFoundException.forReference(hash);
      }

      final Hash ancestor = commit.getAncestor();
      if (previousHash == null) {
        // first commit of the list
        // check if the ancestor of the first commit is currentBranchHash
        foundExpectedHash = foundExpectedHash || expectedHash.equals(ancestor);
      } else {
        // For subsequent commits, check that the lineage is valid
        if (!previousHash.equals(ancestor)) {
          throw new IllegalArgumentException(format("Commit %s is not the ancestor for commit %s", previousHash.asString(), hash));
        }
      }

      foundExpectedHash = foundExpectedHash || expectedHash.equals(hash);
      previousHash = hash;
    }

    final Hash latestCommit = previousHash;

    if (!foundExpectedHash) {
      throw new IllegalArgumentException(format("Commit %s is not an ancestor of commit %s", expectedHash.asString(), latestCommit));
    }

    doAssign(targetBranch, expectedHash, latestCommit);
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch,
      Optional<Hash> expectedBranchHash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedRefHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    requireNonNull(ref);
    requireNonNull(targetHash);
    final Hash expectedHash = checkExpectedHash(ref, expectedRefHash);

    // not locking as there's no support yet for garbage collecting dangling hashes
    if (!commits.containsKey(targetHash)) {
      throw ReferenceNotFoundException.forReference(targetHash);
    }

    doAssign(ref, expectedHash, targetHash);
  }

  private void doAssign(NamedRef ref, Hash expectedHash, final Hash newHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    tryCompute(namedReferences, ref, (key, hash) -> {
      final Hash previousHash = Optional.ofNullable(hash).orElse(expectedHash);
      // Check if the previous and the new value matches
      if (!expectedHash.equals(previousHash)) {
        throw ReferenceConflictException.forReference(ref, Optional.of(expectedHash), Optional.of(previousHash));
      }
      return newHash;
    });
  }


  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    tryCompute(namedReferences, ref, (key, currentHash) -> {
      if (currentHash == null) {
        throw ReferenceNotFoundException.forReference(ref);
      }

      if (!hash.isPresent()) {
        return null;
      }

      if (!hash.equals(Optional.of(currentHash))) {
        throw ReferenceConflictException.forReference(ref, hash, (Optional.of(currentHash)));
      }
      return null;
    });
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return namedReferences
        .entrySet()
        .stream()
        .map(entry -> WithHash.of(entry.getValue(), entry.getKey()));
  }

  @Override
  public Stream<WithHash<MetadataT>> getCommits(Ref ref) throws ReferenceNotFoundException {
    final Hash hash = toHash(ref);
    final Commit<ValueT, MetadataT> commit = commits.get(hash);
    if (commit == null) {
      throw ReferenceNotFoundException.forReference(hash);
    }

    final Iterator<WithHash<MetadataT>> iterator = new CommitsIterator<>(commits::get, hash);

    return Streams.stream(iterator);
  }

  @Override
  public Stream<Key> getKeys(Ref ref) throws ReferenceNotFoundException {
    final Hash hash = toHash(ref);

    final Commit<ValueT, MetadataT> commit = commits.get(hash);
    if (commit == null) {
      throw ReferenceNotFoundException.forReference(hash);
    }

    return commit.getOperations().stream().map(Operation::getKey);
  }

  @Override
  public ValueT getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return getValues(ref, Collections.singletonList(key)).get(0).orElse(null);
  }

  @Override
  public List<Optional<ValueT>> getValues(Ref ref, List<Key> keys) throws ReferenceNotFoundException {
    Hash hash = toHash(ref);
    if (!commits.containsKey(hash)) {
      throw ReferenceNotFoundException.forReference(hash);
    }

    final List<Optional<ValueT>> results = new ArrayList<>(keys.size());
    Collections.fill(results, Optional.empty());

    final Set<Key> toFind = new HashSet<Key>();
    toFind.addAll(keys);

    while (hash != NO_ANCESTOR) {
      if (toFind.isEmpty()) {
        break;
      }

      final Commit<ValueT, MetadataT> commit = commits.get(hash);
      if (commit == null) {
        throw new IllegalStateException("Missing entry for commit " + hash.asString());
      }

      for (Operation<ValueT> operation: commit.getOperations()) {
        final Key operationKey = operation.getKey();
        if (!toFind.contains(operationKey)) {
          continue;
        }

        if (operation instanceof Put) {
          final Put<ValueT> put = (Put<ValueT>) operation;
          int index = keys.indexOf(operationKey);
          results.set(index, Optional.of(put.getValue()));
          toFind.remove(operationKey);
        } else if (operation instanceof Delete) {
          // No need to fill with Optional.empty() as the results were pre-filled
          toFind.remove(operationKey);
        } else if (operation instanceof Unchanged) {
          continue;
        } else {
          throw new AssertionError("Unsupported operation type for " + operation);
        }
      }

      // Trying parent commit
      hash = commit.getAncestor();
    }

    return results;
  }

  @Override
  public Collector collectGarbage() {
    return InactiveCollector.of();
  }

  private Hash checkExpectedHash(NamedRef ref, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    final Hash currentHash = namedReferences.get(ref);
    if (currentHash == null) {
      throw ReferenceNotFoundException.forReference(ref);
    }
    if (!currentHash.equals(expectedHash.orElse(NO_ANCESTOR))) {
      throw ReferenceConflictException.forReference(ref, expectedHash, Optional.of(currentHash));
    }

    return currentHash;
  }

  @FunctionalInterface
  private interface ComputeFunction<K, V> {
    V apply(K k, V v) throws ReferenceNotFoundException, ReferenceConflictException;
  }

  private static <K, V> V tryCompute(ConcurrentMap<K, V> map, K key, ComputeFunction<K, V> doCompute)
      throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      return map.compute(key, (k, v) -> {
        try {
          return doCompute.apply(k, v);
        } catch (ReferenceNotFoundException | ReferenceConflictException e) {
          throw new CompletionException(e);
        }
      });
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      Throwables.throwIfInstanceOf(cause, ReferenceNotFoundException.class);
      Throwables.throwIfInstanceOf(cause, ReferenceConflictException.class);
      throw new AssertionError(e);
    }
  }
}

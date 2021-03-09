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

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.memory.Commit.NO_ANCESTOR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Diff;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.ReferenceAlreadyExistsException;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.Unchanged;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStoreException;
import org.projectnessie.versioned.WithHash;
import org.projectnessie.versioned.WithType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Streams;

/**
 * In-memory implementation of {@code VersionStore} interface.
 *
 * @param <ValueT> Value type
 * @param <MetadataT> Commit metadata type
 * @param <EnumT> the value enum type
 */
public class InMemoryVersionStore<ValueT, MetadataT, EnumT extends Enum<EnumT>> implements VersionStore<ValueT, MetadataT, EnumT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryVersionStore.class);

  private final ConcurrentMap<Hash, Commit<ValueT, MetadataT>> commits = new ConcurrentHashMap<>();
  private final ConcurrentMap<NamedRef, Hash> namedReferences = new ConcurrentHashMap<>();
  private final SerializerWithPayload<ValueT, EnumT> valueSerializer;
  private final Serializer<MetadataT> metadataSerializer;

  public static final class Builder<ValueT, MetadataT, EnumT extends Enum<EnumT>> {
    private SerializerWithPayload<ValueT, EnumT> valueSerializer = null;
    private Serializer<MetadataT> metadataSerializer = null;

    public Builder<ValueT, MetadataT, EnumT> valueSerializer(SerializerWithPayload<ValueT, EnumT> serializer) {
      this.valueSerializer = requireNonNull(serializer);
      return this;
    }

    public Builder<ValueT, MetadataT, EnumT> metadataSerializer(Serializer<MetadataT> serializer) {
      this.metadataSerializer = requireNonNull(serializer);
      return this;
    }

    /**
     * Build a instance of the memory store.
     * @return a memory store instance
     */
    public InMemoryVersionStore<ValueT, MetadataT, EnumT> build() {
      checkState(this.valueSerializer != null, "Value serializer hasn't been set");
      checkState(this.metadataSerializer != null, "Metadata serializer hasn't been set");

      return new InMemoryVersionStore<>(this);
    }
  }

  private InMemoryVersionStore(Builder<ValueT, MetadataT, EnumT> builder) {
    this.valueSerializer = builder.valueSerializer;
    this.metadataSerializer = builder.metadataSerializer;
  }

  /**
   * Create a new in-memory store builder.
   *
   * @param <ValueT> the value type
   * @param <MetadataT> the metadata type
   * @param <EnumT> the value enum type
   * @return a builder for a in memory store
   */
  public static <ValueT, MetadataT, EnumT extends Enum<EnumT>> Builder<ValueT, MetadataT, EnumT> builder() {
    return new Builder<>();
  }

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
    }

    if (ref instanceof Hash) {
      Hash hash = (Hash) ref;
      if (!hash.equals(NO_ANCESTOR) && !commits.containsKey(hash)) {
        throw ReferenceNotFoundException.forReference(hash);
      }
      return hash;
    }

    throw new IllegalArgumentException(format("Unsupported reference type for ref %s", ref));
  }

  private void checkValidReferenceHash(BranchName branch, Hash currentBranchHash, Hash referenceHash)
      throws ReferenceNotFoundException {
    if (referenceHash.equals(NO_ANCESTOR)) {
      return;
    }
    final Optional<Hash> foundHash = Streams.stream(new CommitsIterator<ValueT, MetadataT>(commits::get, currentBranchHash))
        .map(WithHash::getHash)
        .filter(hash -> hash.equals(referenceHash))
        .collect(MoreCollectors.toOptional());

    foundHash.orElseThrow(() -> new ReferenceNotFoundException(format("'%s' hash is not a valid commit from branch '%s'(%s)",
        referenceHash, branch, currentBranchHash)));
  }

  @Override
  public WithHash<Ref> toRef(String refOfUnknownType) throws ReferenceNotFoundException {
    requireNonNull(refOfUnknownType);
    Optional<WithHash<Ref>> result = Stream.<Function<String, Ref>>of(TagName::of, BranchName::of, Hash::of)
        .map(f -> {
          try {
            final Ref ref =  f.apply(refOfUnknownType);
            return WithHash.of(toHash(ref), ref);
          } catch (IllegalArgumentException | ReferenceNotFoundException e) {
            // ignored malformed or nonexistent reference
            return null;
          }
        })
        .filter(Objects::nonNull)
        .findFirst();
    return result.orElseThrow(() -> ReferenceNotFoundException.forReference(refOfUnknownType));
  }

  @Override
  public Hash commit(BranchName branch, Optional<Hash> referenceHash,
      MetadataT metadata, List<Operation<ValueT>> operations) throws ReferenceNotFoundException, ReferenceConflictException {
    final Hash currentHash = toHash(branch);

    // Validate commit
    final List<Key> keys = operations.stream().map(Operation::getKey).distinct().collect(Collectors.toList());
    checkConcurrentModification(branch, currentHash, referenceHash, keys);

    // Storing
    return compute(namedReferences, branch, (key, hash) -> {
      final Commit<ValueT, MetadataT> commit = Commit.of(valueSerializer, metadataSerializer, currentHash, metadata, operations);
      final Hash previousHash = Optional.ofNullable(hash).orElse(NO_ANCESTOR);
      if (!previousHash.equals(currentHash)) {
        // Concurrent modification
        throw ReferenceConflictException.forReference(branch, referenceHash, Optional.of(previousHash));
      }

      // Duplicates are very unlikely and also okay to ignore
      final Hash commitHash = commit.getHash();
      commits.putIfAbsent(commitHash, commit);
      return commitHash;
    });
  }

  @Override
  public void transplant(BranchName targetBranch, Optional<Hash> referenceHash,
      List<Hash> sequenceToTransplant) throws ReferenceNotFoundException, ReferenceConflictException {
    requireNonNull(targetBranch);
    requireNonNull(sequenceToTransplant);

    final Hash currentHash = toHash(targetBranch);

    if (sequenceToTransplant.isEmpty()) {
      return;
    }

    final Set<Key> keys = new HashSet<>();
    final List<Commit<ValueT, MetadataT>> toStore = new ArrayList<>(sequenceToTransplant.size());

    // check that all hashes exist in the store
    Hash ancestor = null;
    Hash newAncestor = currentHash;

    for (final Hash hash: sequenceToTransplant) {
      final Commit<ValueT, MetadataT> commit = commits.get(hash);
      if (commit == null) {
        throw ReferenceNotFoundException.forReference(hash);
      }

      if (ancestor != null && !ancestor.equals(commit.getAncestor())) {
        throw new IllegalArgumentException(format("Hash %s is not the ancestor for commit %s", ancestor, hash));
      }

      commit.getOperations().forEach(op -> keys.add(op.getKey()));

      Commit<ValueT, MetadataT> newCommit = Commit.of(valueSerializer, metadataSerializer,
          newAncestor, commit.getMetadata(), commit.getOperations());
      toStore.add(newCommit);

      ancestor = commit.getHash();
      newAncestor = newCommit.getHash();
    }

    // Validate commit
    checkConcurrentModification(targetBranch, currentHash, referenceHash, new ArrayList<>(keys));

    // Storing
    compute(namedReferences, targetBranch, (key, hash) -> {
      final Hash previousHash = Optional.ofNullable(hash).orElse(NO_ANCESTOR);
      if (!previousHash.equals(currentHash)) {
        // Concurrent modification
        throw ReferenceConflictException.forReference(targetBranch, referenceHash, Optional.of(previousHash));
      }

      toStore.forEach(commit -> commits.putIfAbsent(commit.getHash(), commit));

      final Commit<ValueT, MetadataT> lastCommit = Iterables.getLast(toStore);
      return lastCommit.getHash();
    });
  }

  private void checkConcurrentModification(final BranchName targetBranch, final Hash currentHash, final Optional<Hash> referenceHash,
      final List<Key> keyList) throws ReferenceNotFoundException, ReferenceConflictException {
    // Validate commit
    try {
      ifPresent(referenceHash, hash -> {
        checkValidReferenceHash(targetBranch, currentHash, hash);

        final List<Optional<ValueT>> referenceValues = getValues(hash, keyList);
        final List<Optional<ValueT>> currentValues = getValues(currentHash, keyList);

        if (!referenceValues.equals(currentValues)) {
          throw ReferenceConflictException.forReference(targetBranch, referenceHash, Optional.of(currentHash));
        }
      });
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw e;
    } catch (VersionStoreException e) {
      throw new AssertionError(e);
    }
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedBranchHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    requireNonNull(fromHash);
    requireNonNull(toBranch);
    if (!commits.containsKey(fromHash)) {
      throw ReferenceNotFoundException.forReference(fromHash);
    }
    final Hash currentHash = toHash(toBranch);

    final Hash referenceHash = expectedBranchHash.orElse(currentHash);

    // Find the common ancestor between toBranch and fromHash
    final Set<Hash> toBranchHashes = Streams.stream(new CommitsIterator<>(commits::get, referenceHash))
        .map(WithHash::getHash)
        .collect(Collectors.toSet());

    final Set<Key> keys = new HashSet<>();
    final List<Commit<ValueT, MetadataT>> toMerge = new ArrayList<>();
    Hash commonAncestor = null;
    for (final Iterator<WithHash<Commit<ValueT, MetadataT>>> iterator = new CommitsIterator<ValueT, MetadataT>(
        commits::get, fromHash); iterator.hasNext();) {
      final WithHash<Commit<ValueT, MetadataT>> commit = iterator.next();
      if (toBranchHashes.contains(commit.getHash())) {
        commonAncestor = commit.getHash();
        break;
      }

      toMerge.add(commit.getValue());
      commit.getValue().getOperations().forEach(op -> keys.add(op.getKey()));
    }

    checkConcurrentModification(toBranch, currentHash, expectedBranchHash, new ArrayList<>(keys));

    // Create new commits
    final List<Commit<ValueT, MetadataT>> toStore = new ArrayList<>(toMerge.size());
    Hash newAncestor = currentHash;
    for (final Commit<ValueT, MetadataT> commit : Lists.reverse(toMerge)) {
      final Commit<ValueT, MetadataT> newCommit = Commit.of(valueSerializer, metadataSerializer,
          newAncestor, commit.getMetadata(), commit.getOperations());
      toStore.add(newCommit);
      newAncestor = newCommit.getHash();
    }

    // Storing
    compute(namedReferences, toBranch, (key, hash) -> {
      final Hash previousHash = Optional.ofNullable(hash).orElse(NO_ANCESTOR);
      if (!previousHash.equals(currentHash)) {
        // Concurrent modification
        throw ReferenceConflictException.forReference(toBranch, expectedBranchHash, Optional.of(previousHash));
      }

      toStore.forEach(commit -> commits.putIfAbsent(commit.getHash(), commit));
      final Commit<ValueT, MetadataT> lastCommit = Iterables.getLast(toStore);
      return lastCommit.getHash();
    });
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedRefHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    requireNonNull(ref);
    requireNonNull(targetHash);

    final Hash currentHash = toHash(ref);

    ifPresent(expectedRefHash, hash -> {
      if (!hash.equals(currentHash)) {
        throw ReferenceConflictException.forReference(ref, expectedRefHash, Optional.of(currentHash));
      }
    });

    // not locking as there's no support yet for garbage collecting dangling hashes
    if (!commits.containsKey(targetHash)) {
      throw ReferenceNotFoundException.forReference(targetHash);
    }

    doAssign(ref, currentHash, targetHash);
  }

  private void doAssign(NamedRef ref, Hash expectedHash, final Hash newHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    compute(namedReferences, ref, (key, hash) -> {
      final Hash previousHash = Optional.ofNullable(hash).orElse(expectedHash);
      // Check if the previous and the new value matches
      if (!expectedHash.equals(previousHash)) {
        throw ReferenceConflictException.forReference(ref, Optional.of(expectedHash), Optional.of(previousHash));
      }
      return newHash;
    });
  }

  @Override
  public Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    Preconditions.checkArgument(ref instanceof BranchName || targetHash.isPresent(), "Cannot create an unassigned tag reference");

    return compute(namedReferences, ref, (key, currentHash) -> {
      if (currentHash != null) {
        throw ReferenceAlreadyExistsException.forReference(ref);
      }

      return targetHash.orElse(NO_ANCESTOR);
    });
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash) throws ReferenceNotFoundException, ReferenceConflictException {
    try {
      compute(namedReferences, ref, (key, currentHash) -> {
        if (currentHash == null) {
          throw ReferenceNotFoundException.forReference(ref);
        }

        ifPresent(hash, h -> {
          if (!h.equals(currentHash)) {
            throw ReferenceConflictException.forReference(ref, hash, (Optional.of(currentHash)));
          }
        });

        return null;
      });
    } catch (ReferenceNotFoundException | ReferenceConflictException e) {
      throw e;
    } catch (VersionStoreException e) {
      throw new AssertionError(e);
    }
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

    final Iterator<WithHash<Commit<ValueT, MetadataT>>> iterator = new CommitsIterator<>(commits::get, hash);
    return Streams.stream(iterator).map(wh -> WithHash.of(wh.getHash(), wh.getValue().getMetadata()));
  }

  @Override
  public Stream<WithType<Key, EnumT>> getKeys(Ref ref) throws ReferenceNotFoundException {
    final Hash hash = toHash(ref);

    final Iterator<WithHash<Commit<ValueT, MetadataT>>> iterator = new CommitsIterator<>(commits::get, hash);
    final Set<Key> deleted = new HashSet<>();
    return Streams.stream(iterator)
        // flatten the operations (in reverse order)
        .flatMap(wh -> Lists.reverse(wh.getValue().getOperations()).stream())
        // block deleted keys
        .filter(operation -> {
          Key key = operation.getKey();
          if (operation instanceof Delete) {
            deleted.add(key);
          }
          return !deleted.contains(key);
        })
        .filter(Put.class::isInstance)
        // extract the keys
        .map(o -> WithType.of(valueSerializer.getType(((Put<ValueT>) o).getValue()), o.getKey()))
        // filter keys which have been seen already
        .collect(Collectors.toMap(WithType::getValue, Function.identity(), (k1, k2) -> k2))
        .values()
        .stream();
  }

  @Override
  public ValueT getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return getValues(ref, Collections.singletonList(key)).get(0).orElse(null);
  }

  @Override
  public List<Optional<ValueT>> getValues(Ref ref, List<Key> keys) throws ReferenceNotFoundException {
    final Hash hash = toHash(ref);

    final int size = keys.size();
    final List<Optional<ValueT>> results = new ArrayList<>(size);
    results.addAll(Collections.nCopies(size, Optional.empty()));

    final Set<Key> toFind = new HashSet<Key>();
    toFind.addAll(keys);

    final Iterator<WithHash<Commit<ValueT, MetadataT>>> iterator = new CommitsIterator<>(commits::get, hash);
    while (iterator.hasNext()) {
      if (toFind.isEmpty()) {
        // early exit if all keys have been found
        break;
      }

      final Commit<ValueT, MetadataT> commit = iterator.next().getValue();
      for (Operation<ValueT> operation: Lists.reverse(commit.getOperations())) {
        final Key operationKey = operation.getKey();
        // ignore keys of no interest
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
    }

    return results;
  }

  @Override
  public Stream<Diff<ValueT>> getDiffs(Ref from, Ref to) {
    throw new UnsupportedOperationException("Not yet implemented.");
  }

  @Override
  public Collector collectGarbage() {
    return InactiveCollector.of();
  }

  /**
   * For testing purposes only, clears the {@link #commits} and {@link #namedReferences} maps and creates a new {@code main} branch.
   */
  @VisibleForTesting
  public void clearUnsafe() {
    commits.clear();
    namedReferences.clear();
    // Hint: "main" is hard-coded here
    try {
      create(BranchName.of("main"), Optional.empty());
    } catch (ReferenceNotFoundException | ReferenceAlreadyExistsException e) {
      throw new RuntimeException("Failed to reset the InMemoryVersionStore for tests");
    }
  }

  @SuppressWarnings("serial")
  private static class VersionStoreExecutionError extends Error {
    private VersionStoreExecutionError(VersionStoreException cause) {
      super(cause);
    }

    @Override
    public synchronized VersionStoreException getCause() {
      return (VersionStoreException) super.getCause();
    }
  }

  @FunctionalInterface
  private interface ComputeFunction<K, V, E extends VersionStoreException> {
    V apply(K k, V v) throws E;
  }

  @FunctionalInterface
  private interface IfPresentConsumer<V, E extends VersionStoreException> {
    void accept(V v) throws E;
  }

  private static <K, V, E extends VersionStoreException> V compute(ConcurrentMap<K, V> map, K key, ComputeFunction<K, V, E> doCompute)
      throws E {
    try {
      return map.compute(key, (k, v) -> {
        try {
          return doCompute.apply(k, v);
        } catch (VersionStoreException e) {
          // e is of type E but cannot catch a generic type
          throw new VersionStoreExecutionError(e);
        }
      });
    } catch (VersionStoreExecutionError e) {
      @SuppressWarnings("unchecked")
      E cause = (E) e.getCause();
      throw cause;
    }
  }

  private static <T, E extends VersionStoreException> void ifPresent(Optional<T> optional, IfPresentConsumer<? super T, E> consumer)
      throws E {
    try {
      optional.ifPresent(value -> {
        try {
          consumer.accept(value);
        } catch (VersionStoreException e) {
          // e is of type E but cannot catch a generic type
          throw new VersionStoreExecutionError(e);
        }
      });
    } catch (VersionStoreExecutionError e) {
      @SuppressWarnings("unchecked")
      E cause = (E) e.getCause();
      throw cause;
    }
  }
}

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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class BackwardsCompatibleVersionStore<VALUE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>>
    implements VersionStore<VALUE, METADATA, VALUE_TYPE> {
  private final VersionStore<VALUE, METADATA, VALUE_TYPE> delegate;
  private final SerializerWithPayload<VALUE, VALUE_TYPE> valueSerializer;

  public BackwardsCompatibleVersionStore(
      VersionStore<VALUE, METADATA, VALUE_TYPE> delegate,
      SerializerWithPayload<VALUE, VALUE_TYPE> valueSerializer) {
    this.valueSerializer = valueSerializer;
    this.delegate = delegate;
  }

  @Override
  public Hash commit(
      @Nonnull BranchName branch,
      @Nonnull Optional<Hash> referenceHash,
      @Nonnull METADATA metadata,
      @Nonnull List<Operation<VALUE>> operations)
      throws ReferenceNotFoundException, ReferenceConflictException {
    // TODO fleshed out with global-states PR
    return delegate.commit(branch, referenceHash, metadata, operations);
  }

  @Override
  public Stream<WithType<Key, VALUE_TYPE>> getKeys(Ref ref) throws ReferenceNotFoundException {
    return delegate.getKeys(ref).map(this::mapKey);
  }

  @Override
  public VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException {
    return mapValue(delegate.getValue(ref, key));
  }

  @Override
  public List<Optional<VALUE>> getValues(Ref ref, List<Key> keys)
      throws ReferenceNotFoundException {
    return delegate.getValues(ref, keys).stream()
        .map(v -> v.map(this::mapValue))
        .collect(Collectors.toList());
  }

  protected VALUE mapValue(VALUE value) {
    return value; // TODO updated later w/ global-states-PR
  }

  private WithType<Key, VALUE_TYPE> mapKey(WithType<Key, VALUE_TYPE> keyWithType) {
    return keyWithType; // TODO updated later w/ global-states-PR
  }

  //

  @Override
  public Hash hashOnReference(NamedRef namedReference, Optional<Hash> hashOnReference)
      throws ReferenceNotFoundException {
    return delegate.hashOnReference(namedReference, hashOnReference);
  }

  @Override
  @Nonnull
  public Hash noAncestorHash() {
    return delegate.noAncestorHash();
  }

  @Override
  @Nonnull
  public Hash toHash(@Nonnull NamedRef ref) throws ReferenceNotFoundException {
    return delegate.toHash(ref);
  }

  @Override
  public WithHash<Ref> toRef(@Nonnull String refOfUnknownType) throws ReferenceNotFoundException {
    return delegate.toRef(refOfUnknownType);
  }

  @Override
  public void transplant(
      BranchName targetBranch, Optional<Hash> referenceHash, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    delegate.transplant(targetBranch, referenceHash, sequenceToTransplant);
  }

  @Override
  public void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    delegate.merge(fromHash, toBranch, expectedHash);
  }

  @Override
  public void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    delegate.assign(ref, expectedHash, targetHash);
  }

  @Override
  public Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    return delegate.create(ref, targetHash);
  }

  @Override
  public void delete(NamedRef ref, Optional<Hash> hash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    delegate.delete(ref, hash);
  }

  @Override
  public Stream<WithHash<NamedRef>> getNamedRefs() {
    return delegate.getNamedRefs();
  }

  @Override
  public Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException {
    return delegate.getCommits(ref);
  }

  @Override
  public Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException {
    return delegate.getDiffs(from, to);
  }

  @Override
  public Collector collectGarbage() {
    return delegate.collectGarbage();
  }
}

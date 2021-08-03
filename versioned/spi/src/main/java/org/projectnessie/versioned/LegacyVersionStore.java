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
import java.util.stream.StreamSupport;

/**
 * <em>Temporary</em> bridge between the updated {@link VersionStore} forcing the use of named-refs
 * and legacy {@link VersionStore} implementations.
 *
 * <p>This class will be removed once everything else for PR #1638 has been merged and the legacy
 * version-store implementations are eventually replaced.
 */
public abstract class LegacyVersionStore<VALUE, METADATA, VALUE_TYPE extends Enum<VALUE_TYPE>>
    implements VersionStore<VALUE, METADATA, VALUE_TYPE> {

  private volatile Hash emptyHash;

  protected final Hash emptyHash() {
    if (emptyHash == null) {
      try {
        BranchName branch = BranchName.of("__determine__empty__hash__");
        emptyHash = create(branch, Optional.empty());
        delete(branch, Optional.of(emptyHash));
      } catch (ReferenceNotFoundException
          | ReferenceAlreadyExistsException
          | ReferenceConflictException e) {
        throw new RuntimeException(e);
      }
    }
    return emptyHash;
  }

  Optional<Hash> refWithHashToOnlyHash(Optional<NamedRef> ref, Optional<Hash> hash)
      throws ReferenceNotFoundException {
    if (hash.isPresent()) {
      if (ref.isPresent()) {
        if (emptyHash().equals(hash.get())) {
          // NO_ANCESTOR / beginning of time
          return hash;
        }

        // we need to make sure that the hash in fact exists on the named ref
        try (Stream<WithHash<METADATA>> commits = getCommits(ref.get())) {
          if (commits.noneMatch(c -> c.getHash().equals(hash.get()))) {
            throw new ReferenceNotFoundException(
                String.format(
                    "Hash %s on Ref %s could not be found",
                    hash.get().asString(), ref.get().getName()));
          }
        }
      }
      return hash;
    }
    if (ref.isPresent()) {
      return Optional.of(toRef(ref.get().getName()).getHash());
    }
    return Optional.empty();
  }

  @Override
  public final Hash create(NamedRef ref, Optional<NamedRef> target, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException {
    if (ref instanceof TagName && !targetHash.isPresent()) {
      throw new IllegalArgumentException("Cannot create an unassigned tag reference");
    }

    Optional<Hash> legacyRef = refWithHashToOnlyHash(target, targetHash);

    return create(ref, legacyRef);
  }

  public abstract Hash create(NamedRef ref, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceAlreadyExistsException;

  @Override
  public final Stream<WithHash<METADATA>> getCommits(
      NamedRef ref, Optional<Hash> offset, Optional<Hash> untilIncluding)
      throws ReferenceNotFoundException {

    Optional<Hash> legacyRef = refWithHashToOnlyHash(Optional.of(ref), offset);

    Stream<WithHash<METADATA>> stream =
        getCommits(legacyRef.orElseThrow(IllegalArgumentException::new));

    if (untilIncluding.isPresent()) {
      stream =
          StreamSupport.stream(
              StreamUtil.takeUntilIncl(
                  stream.spliterator(), x -> x.getHash().equals(untilIncluding.get())),
              false);
    }

    return stream;
  }

  public abstract Stream<WithHash<METADATA>> getCommits(Ref ref) throws ReferenceNotFoundException;

  @Override
  public final Stream<WithType<Key, VALUE_TYPE>> getKeys(NamedRef ref, Optional<Hash> hashOnRef)
      throws ReferenceNotFoundException {
    Optional<Hash> legacyRef = refWithHashToOnlyHash(Optional.of(ref), hashOnRef);

    return getKeys(legacyRef.orElseThrow(IllegalArgumentException::new));
  }

  public abstract Stream<WithType<Key, VALUE_TYPE>> getKeys(Ref ref)
      throws ReferenceNotFoundException;

  @Override
  public final VALUE getValue(NamedRef ref, Optional<Hash> hashOnRef, Key key)
      throws ReferenceNotFoundException {
    Optional<Hash> legacyRef = refWithHashToOnlyHash(Optional.of(ref), hashOnRef);

    return getValue(legacyRef.orElseThrow(IllegalArgumentException::new), key);
  }

  public abstract VALUE getValue(Ref ref, Key key) throws ReferenceNotFoundException;

  @Override
  public final List<Optional<VALUE>> getValues(
      NamedRef ref, Optional<Hash> hashOnRef, List<Key> keys) throws ReferenceNotFoundException {
    Optional<Hash> legacyRef = refWithHashToOnlyHash(Optional.of(ref), hashOnRef);

    return getValues(legacyRef.orElseThrow(IllegalArgumentException::new), keys);
  }

  public abstract List<Optional<VALUE>> getValues(Ref ref, List<Key> keys)
      throws ReferenceNotFoundException;

  @Override
  public Hash transplant(
      BranchName targetBranch,
      Optional<Hash> referenceHash,
      NamedRef source,
      List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException {
    transplant(targetBranch, referenceHash, sequenceToTransplant);

    return toRef(targetBranch.getName()).getHash();
  }

  public abstract void transplant(
      BranchName targetBranch, Optional<Hash> expectedHash, List<Hash> sequenceToTransplant)
      throws ReferenceNotFoundException, ReferenceConflictException;

  @Override
  public final Hash merge(
      NamedRef from, Optional<Hash> fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Optional<Hash> legacyRefFrom = refWithHashToOnlyHash(Optional.of(from), fromHash);

    merge(legacyRefFrom.orElseThrow(IllegalArgumentException::new), toBranch, expectedHash);

    return toRef(toBranch.getName()).getHash();
  }

  public abstract void merge(Hash fromHash, BranchName toBranch, Optional<Hash> expectedHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  @Override
  public final void assign(
      NamedRef ref, Optional<Hash> expectedHash, NamedRef hashOnRef, Optional<Hash> targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Optional<Hash> legacyRef = refWithHashToOnlyHash(Optional.of(hashOnRef), targetHash);

    assign(ref, expectedHash, legacyRef.orElseThrow(IllegalArgumentException::new));
  }

  public abstract void assign(NamedRef ref, Optional<Hash> expectedHash, Hash targetHash)
      throws ReferenceNotFoundException, ReferenceConflictException;

  @Override
  public final Stream<Diff<VALUE>> getDiffs(
      NamedRef from, Optional<Hash> hashOnFrom, NamedRef to, Optional<Hash> hashOnTo)
      throws ReferenceNotFoundException {
    Optional<Hash> legacyRefFrom = refWithHashToOnlyHash(Optional.of(from), hashOnFrom);
    Optional<Hash> legacyRefTo = refWithHashToOnlyHash(Optional.of(to), hashOnTo);

    return getDiffs(
        legacyRefFrom.orElseThrow(IllegalArgumentException::new),
        legacyRefTo.orElseThrow(IllegalArgumentException::new));
  }

  public abstract Stream<Diff<VALUE>> getDiffs(Ref from, Ref to) throws ReferenceNotFoundException;
}

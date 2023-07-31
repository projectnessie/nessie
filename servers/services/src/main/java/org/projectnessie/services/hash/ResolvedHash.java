/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.hash;

import static com.google.common.base.Preconditions.checkState;

import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.versioned.DetachedRef;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.NamedRef;
import org.projectnessie.versioned.WithHash;

/**
 * A {@link Hash} that has been resolved against a {@link NamedRef}.
 *
 * <p>It extends {@link WithHash} to provide compatibility with legacy code.
 */
@Value.Immutable
@SuppressWarnings("immutables:subtype")
public interface ResolvedHash extends WithHash<NamedRef> {

  /** The {@link NamedRef}; can be {@link DetachedRef}. */
  NamedRef getNamedRef();

  /**
   * The user-provided hash. Only present if a hash was actually provided and successfully parsed;
   * will be empty if the hash was implicitly resolved to the ref's HEAD.
   *
   * <p>If this is empty, then {@link #getHead()} is guaranteed to be non-empty.
   *
   * <p>If the provided hash had relative specs, this hash will be the result of resolving those
   * specs against its absolute part, or the ref's HEAD if it doesn't have an absolute part.
   */
  Optional<Hash> getProvidedHash();

  /**
   * The ref's HEAD, if available. Will always be empty for {@link DetachedRef}. Exposed mostly to
   * avoid re-fetching the HEAD many times.
   *
   * <p>If this is empty, then {@link #getProvidedHash()} is guaranteed to be non-empty.
   */
  Optional<Hash> getHead();

  /**
   * The effective resolved hash; never null. Will either be {@link #getProvidedHash()} if present,
   * or otherwise, {@link #getHead()}.
   */
  @Override
  Hash getHash();

  @Value.Derived
  @Override
  default NamedRef getValue() {
    return getNamedRef();
  }

  static ResolvedHash of(NamedRef ref, Optional<Hash> providedHash, Optional<Hash> head) {
    checkState(
        providedHash.isPresent() || head.isPresent(),
        "Either providedHash or head must be present");
    Hash effective = providedHash.orElseGet(head::get);
    return ImmutableResolvedHash.builder()
        .namedRef(ref)
        .providedHash(providedHash)
        .head(head)
        .hash(effective)
        .build();
  }
}

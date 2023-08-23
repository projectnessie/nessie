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
   * The ref's HEAD, if available. Will always be empty for {@link DetachedRef}. Exposed mostly to
   * avoid re-fetching the HEAD many times.
   */
  Optional<Hash> getHead();

  /** The effective resolved hash, never {@code null}. */
  @Override
  Hash getHash();

  @Value.NonAttribute
  @Override
  default NamedRef getValue() {
    return getNamedRef();
  }

  static ResolvedHash of(NamedRef ref, Optional<Hash> head, Hash resolved) {
    return ImmutableResolvedHash.builder().namedRef(ref).head(head).hash(resolved).build();
  }
}

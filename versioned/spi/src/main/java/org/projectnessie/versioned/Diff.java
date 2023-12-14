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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nullable;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;

@Value.Immutable
public interface Diff {

  @Value.Parameter(order = 1)
  @Nullable
  IdentifiedContentKey getFromKey();

  @Value.Parameter(order = 2)
  @Nullable
  IdentifiedContentKey getToKey();

  @Value.Parameter(order = 3)
  Optional<Content> getFromValue();

  @Value.Parameter(order = 4)
  Optional<Content> getToValue();

  @Value.NonAttribute
  default ContentKey contentKey() {
    IdentifiedContentKey k = getFromKey();
    if (k == null) {
      k = getToKey();
    }
    return requireNonNull(k).contentKey();
  }

  @Value.Check
  default void check() {
    IdentifiedContentKey from = getFromKey();
    IdentifiedContentKey to = getToKey();
    if (from != null && to != null) {
      checkArgument(
          from.contentKey().equals(to.contentKey()),
          "ContentKeys for from (%s) and to (%s) keys must be equal",
          from.contentKey(),
          to.contentKey());
    }
  }

  static Diff of(
      IdentifiedContentKey fromKey,
      IdentifiedContentKey toKey,
      Optional<Content> from,
      Optional<Content> to) {
    return ImmutableDiff.of(fromKey, toKey, from, to);
  }
}

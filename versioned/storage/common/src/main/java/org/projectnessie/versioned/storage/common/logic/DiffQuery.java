/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.logic;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import java.util.function.Predicate;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;

@Value.Immutable
public interface DiffQuery extends PageableQuery {

  @Override
  @Value.Parameter(order = 1)
  Optional<PagingToken> pagingToken();

  @Nullable
  @Value.Parameter(order = 2)
  CommitObj fromCommit();

  @Nullable
  @Value.Parameter(order = 3)
  CommitObj toCommit();

  /**
   * Optional start condition.
   *
   * <p><em>Prefix queries: </em> {@code begin} and {@code end} must be equal and not {@code null},
   * only elements that start with the given key value will be returned.
   *
   * <p><em>Start at queries: </em>Start at {@code begin} (inclusive)
   *
   * <p><em>End at queries: </em>End at {@code end} (inclusive if exact match) restrictions
   *
   * <p><em>Range queries: </em>{@code begin} (inclusive) and {@code end} (inclusive if exact match)
   * restrictions
   */
  @Nullable
  @Value.Parameter(order = 4)
  StoreKey start();

  /** Optional start condition, see {@link #start()}. */
  @Nullable
  @Value.Parameter(order = 5)
  StoreKey end();

  /**
   * Eager prefetch of all potentially required indexes.
   *
   * <p>Set to {@code false}, when using result paging.
   */
  @Value.Parameter(order = 6)
  boolean prefetch();

  @Nullable
  @Value.Parameter(order = 7)
  Predicate<StoreKey> filter();

  @Nonnull
  static DiffQuery diffQuery(
      @Nullable PagingToken pagingToken,
      @Nullable CommitObj fromCommit,
      @Nullable CommitObj toCommit,
      @Nullable StoreKey start,
      @Nullable StoreKey end,
      boolean prefetch,
      @Nullable Predicate<StoreKey> filter) {
    return ImmutableDiffQuery.of(
        Optional.ofNullable(pagingToken), fromCommit, toCommit, start, end, prefetch, filter);
  }

  @Nonnull
  static DiffQuery diffQuery(
      @Nullable CommitObj fromCommit,
      @Nullable CommitObj toCommit,
      boolean prefetch,
      @Nullable Predicate<StoreKey> filter) {
    return diffQuery(null, fromCommit, toCommit, null, null, prefetch, filter);
  }
}

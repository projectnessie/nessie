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

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@Value.Immutable
public interface CommitLogQuery extends PageableQuery {

  @Override
  @Value.Parameter(order = 1)
  Optional<PagingToken> pagingToken();

  @Value.Parameter(order = 2)
  ObjId commitId();

  @Value.Parameter(order = 3)
  Optional<ObjId> endCommitId();

  @Nonnull
  @jakarta.annotation.Nonnull
  static CommitLogQuery commitLogQuery(@Nonnull @jakarta.annotation.Nonnull ObjId commitId) {
    return commitLogQuery(null, commitId, null);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  static CommitLogQuery commitLogQuery(
      @Nullable @jakarta.annotation.Nullable PagingToken pagingToken,
      @Nonnull @jakarta.annotation.Nonnull ObjId commitId,
      @Nullable @jakarta.annotation.Nullable ObjId endCommitId) {
    return ImmutableCommitLogQuery.of(
        Optional.ofNullable(pagingToken), commitId, Optional.ofNullable(endCommitId));
  }
}

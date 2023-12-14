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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.UUID;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitType;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * Helper to create a commit without dealing with the internals, like index handling, of {@link
 * CommitObj}.
 */
@Value.Immutable
public interface CreateCommit {
  ObjId parentCommitId();

  List<ObjId> secondaryParents();

  CommitHeaders headers();

  String message();

  List<Add> adds();

  List<Unchanged> unchanged();

  List<Remove> removes();

  @Value.Default
  default CommitType commitType() {
    return CommitType.NORMAL;
  }

  static Builder newCommitBuilder() {
    return ImmutableCreateCommit.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder parentCommitId(ObjId parentCommitId);

    @CanIgnoreReturnValue
    Builder addSecondaryParents(ObjId element);

    @CanIgnoreReturnValue
    Builder headers(CommitHeaders headers);

    @CanIgnoreReturnValue
    Builder message(String message);

    @CanIgnoreReturnValue
    Builder addAdds(CreateCommit.Add element);

    @CanIgnoreReturnValue
    Builder addUnchanged(CreateCommit.Unchanged element);

    @CanIgnoreReturnValue
    Builder addRemoves(CreateCommit.Remove element);

    Builder commitType(CommitType commitType);

    CreateCommit build();
  }

  @Value.Immutable
  interface Add {
    @Value.Parameter(order = 1)
    StoreKey key();

    @Value.Parameter(order = 2)
    int payload();

    @Value.Parameter(order = 3)
    ObjId value();

    @Value.Parameter(order = 4)
    @Nullable
    ObjId expectedValue();

    // Note: the content-ID from legacy, imported Nessie repositories could theoretically been
    // any string value. If it's a UUID, use it, otherwise ignore it down the road.
    @Value.Parameter(order = 5)
    @Nullable
    UUID contentId();

    static Add commitAdd(
        @Nonnull StoreKey key,
        int payload,
        @Nonnull ObjId value,
        @Nullable ObjId expectedValue,
        @Nullable UUID contentId) {
      checkArgument(payload >= 0 && payload <= 127);
      return ImmutableAdd.of(key, payload, value, expectedValue, contentId);
    }
  }

  @Value.Immutable
  interface Unchanged {
    @Value.Parameter(order = 1)
    StoreKey key();

    @Value.Parameter(order = 2)
    int payload();

    @Value.Parameter(order = 3)
    @Nullable
    ObjId expectedValue();

    // Note: the content-ID from legacy, imported Nessie repositories could theoretically been
    // any string value. If it's a UUID, use it, otherwise ignore it down the road.
    @Value.Parameter(order = 4)
    @Nullable
    UUID contentId();

    static Unchanged commitUnchanged(
        @Nonnull StoreKey key,
        int payload,
        @Nullable ObjId expectedValue,
        @Nullable UUID contentId) {
      checkArgument(payload >= 0 && payload <= 127);
      return ImmutableUnchanged.of(key, payload, expectedValue, contentId);
    }
  }

  @Value.Immutable
  interface Remove {
    @Value.Parameter(order = 1)
    StoreKey key();

    @Value.Parameter(order = 2)
    int payload();

    @Value.Parameter(order = 3)
    ObjId expectedValue();

    @Value.Parameter(order = 4)
    @Nullable
    UUID contentId();

    static Remove commitRemove(
        @Nonnull StoreKey key,
        int payload,
        @Nonnull ObjId expectedValue,
        @Nullable UUID contentId) {
      checkArgument(payload >= 0 && payload <= 127);
      return ImmutableRemove.of(key, payload, expectedValue, contentId);
    }
  }
}

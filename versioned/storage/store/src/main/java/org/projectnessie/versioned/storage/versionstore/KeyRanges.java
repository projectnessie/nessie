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
package org.projectnessie.versioned.storage.versionstore;

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.logic.PagingToken.fromString;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKeyMax;
import static org.projectnessie.versioned.storage.versionstore.TypeMapping.keyToStoreKeyMin;

import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.VersionStore.KeyRestrictions;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.logic.PagingToken;

@Value.Immutable
abstract class KeyRanges {
  static KeyRanges keyRanges(String pagingToken, KeyRestrictions keyRestrictions) {
    return ImmutableKeyRanges.of(
        pagingToken,
        keyRestrictions.minKey(),
        keyRestrictions.maxKey(),
        keyRestrictions.prefixKey());
  }

  @Value.Parameter(order = 1)
  @Nullable
  abstract String pagingToken();

  @Value.Parameter(order = 2)
  @Nullable
  abstract ContentKey minKey();

  @Value.Parameter(order = 3)
  @Nullable
  abstract ContentKey maxKey();

  @Value.Parameter(order = 4)
  @Nullable
  abstract ContentKey prefixKey();

  @Value.NonAttribute
  StoreKey beginStoreKey() {
    String paging = pagingToken();
    if (paging != null && !paging.isEmpty()) {
      return keyFromString(fromString(paging).token().toStringUtf8());
    }
    ContentKey prefix = prefixKey();
    ContentKey min = (prefix != null) ? prefix : minKey();
    return min != null ? keyToStoreKeyMin(min) : null;
  }

  @Value.NonAttribute
  PagingToken pagingTokenObj() {
    String paging = pagingToken();
    return paging != null ? fromString(paging) : null;
  }

  @Value.NonAttribute
  StoreKey endStoreKey() {
    ContentKey max = maxKey();
    return max != null ? keyToStoreKeyMax(max) : null;
  }

  @Value.Check
  void validate() {
    if (prefixKey() != null) {
      checkArgument(
          minKey() == null && maxKey() == null,
          "Combining prefixKey with either minKey or maxKey is not supported.");
    }
  }
}

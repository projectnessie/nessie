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
package org.projectnessie.versioned.persist.adapter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

/** Composite for the per-named-reference and global state for a content key. */
@Value.Immutable
public interface ContentAndState {

  @Value.Parameter(order = 1)
  byte getPayload();

  /**
   * Per-named-reference state for a content key. For example, Iceberg's snapshot-ID, schema-ID,
   * partition-spec-ID, default-sort-order-ID.
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  @Value.Parameter(order = 2)
  ByteString getRefState();

  /** Global state for a content key. For example, the pointer to Iceberg's table-metadata. */
  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 3)
  ByteString getGlobalState();

  @Nonnull
  @jakarta.annotation.Nonnull
  static ContentAndState of(
      byte payload,
      @Nonnull @jakarta.annotation.Nonnull ByteString refState,
      @Nullable @jakarta.annotation.Nullable ByteString globalState) {
    return ImmutableContentAndState.of(payload, refState, globalState);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  static ContentAndState of(
      byte payload, @Nonnull @jakarta.annotation.Nonnull ByteString refState) {
    return ImmutableContentAndState.of(payload, refState, null);
  }
}

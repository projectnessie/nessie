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
import org.immutables.value.Value.Immutable;

/** Composite for the per-named-reference and global state for a contents key. */
@Immutable
public interface ContentsAndState<CONTENTS> {

  /** Per-named-reference state for a contents key. For example, Iceberg's snapshot-ID. */
  @Nonnull
  CONTENTS getRefState();

  /** Global state for a contents key. For example, the pointer to Iceberg's table-metadata. */
  @Nullable
  CONTENTS getGlobalState();

  @Nonnull
  static <CONTENTS> ContentsAndState<CONTENTS> of(
      @Nonnull CONTENTS refState, @Nonnull CONTENTS globalState) {
    return ImmutableContentsAndState.<CONTENTS>builder()
        .refState(refState)
        .globalState(globalState)
        .build();
  }

  @Nonnull
  static <CONTENTS> ContentsAndState<CONTENTS> of(@Nonnull CONTENTS refState) {
    return ImmutableContentsAndState.<CONTENTS>builder().refState(refState).build();
  }
}

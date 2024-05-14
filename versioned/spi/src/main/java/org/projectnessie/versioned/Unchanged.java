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

import jakarta.annotation.Nonnull;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;

/**
 * An operation when ensures that a value has been unchanged since the expected hash for a commit.
 * Always expects to match hash since this is otherwise a no-op. Can be used to enforce serialized
 * transaction isolation confirming that no operations have occurred to the provided key since the
 * operation stated.
 */
@Value.Immutable
public interface Unchanged extends Operation {
  @Override
  default OperationType getType() {
    return OperationType.UNCHANGED;
  }

  @Override
  default boolean shouldMatchHash() {
    return true;
  }

  /**
   * Creates a unchanged operation for the given key.
   *
   * @param key the key impacted by the operation
   * @return a unchanged operation for the key
   */
  @Nonnull
  static Unchanged of(@Nonnull ContentKey key) {
    return ImmutableUnchanged.builder().key(key).build();
  }
}

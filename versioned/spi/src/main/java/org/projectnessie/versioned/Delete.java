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

/** A delete operation. */
@Value.Immutable
public interface Delete extends Operation {

  @Override
  default OperationType getType() {
    return OperationType.DELETE;
  }

  /**
   * Creates a delete operation for the given key.
   *
   * <p>If the key for a content shall change (aka a rename), then use a {@link Delete} operation
   * using the current (old) key and a {@link Put} operation using the new key providing the {@code
   * value} with the correct content ID.
   *
   * @param key the key impacted by the operation
   * @return a delete operation for the key
   */
  @Nonnull
  static Delete of(@Nonnull ContentKey key) {
    return ImmutableDelete.builder().key(key).build();
  }
}

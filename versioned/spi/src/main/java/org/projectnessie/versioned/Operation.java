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

import org.immutables.value.Value;

public interface Operation {
  /**
   * Whether the commit expected hash should be reviewed to confirm the key for this operation
   * hasn't changed since the expected hash.
   *
   * @return True if this operation should match the hash.
   */
  @Value.Default
  default boolean shouldMatchHash() {
    return true;
  }

  /** The key for this operation. */
  @Value.Parameter(order = 1)
  Key getKey();
}

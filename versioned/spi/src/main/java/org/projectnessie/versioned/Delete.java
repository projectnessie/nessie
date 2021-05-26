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

import javax.annotation.Nonnull;
import org.immutables.value.Value;

/** A delete operation. */
@Value.Immutable
public interface Delete<V> extends Operation<V> {

  /**
   * Creates a delete operation for the given key.
   *
   * @param <V> the store value type
   * @param key the key impacted by the operation
   * @return a delete operation for the key
   */
  @Nonnull
  static <V> Delete<V> of(@Nonnull Key key) {
    return ImmutableDelete.<V>builder().key(key).build();
  }
}

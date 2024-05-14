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
package org.projectnessie.versioned.storage.common.indexes;

import jakarta.annotation.Nonnull;

@FunctionalInterface
public interface IndexLoader<V> {
  static <V> IndexLoader<V> notLoading() {
    return indexes -> {
      throw new UnsupportedOperationException("not loading");
    };
  }

  /**
   * Load the given indexes, {@code null} elements might be present in the {@code indexes}
   * parameters.
   *
   * @return a new array of the same length as the input array, with the non-{@code null} elements
   *     of the input parameter set to the loaded indexes. Loaded indexes may or may not be the same
   *     (input) instance.
   */
  @Nonnull
  StoreIndex<V>[] loadIndexes(@Nonnull StoreIndex<V>[] indexes);
}

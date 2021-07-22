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
import javax.annotation.Nullable;
import org.immutables.value.Value;

/** Setting a new value. Can optionally declare whether the prior hash must match. */
@Value.Immutable
public interface Put<V, S> extends Operation<V, S> {

  /**
   * The value to store for this operation.
   *
   * @return the value
   */
  V getValue();

  /** The expected global state for this operation. */
  @Nullable
  S getExpectedState();

  /**
   * Creates a put operation for the given key and value.
   *
   * @param <V> the store value type
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  static <V, S> Put<V, S> of(@Nonnull Key key, @Nonnull V value) {
    return ImmutablePut.<V, S>builder().key(key).value(value).build();
  }

  @Nonnull
  static <V, S> Put<V, S> of(@Nonnull Key key, @Nonnull V value, @Nullable S expectedState) {
    return ImmutablePut.<V, S>builder().key(key).value(value).expectedState(expectedState).build();
  }
}

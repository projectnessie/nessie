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
public interface Put<V> extends Operation<V> {

  /**
   * The value to store for this operation.
   *
   * @return the value
   */
  V getValue();

  @Nullable
  V getExpectedValue();

  /**
   * Creates a put-operation for the given key and value without an expected-value so the returned
   * put-operation is unconditional.
   *
   * <p>Unconditional put-operations must be used for contents-types that do not support
   * global-state and for those that do support global-state when a new contents object is added.
   *
   * @param <V> the store value type
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  public static <V> Put<V> of(@Nonnull Key key, @Nonnull V value) {
    return ImmutablePut.<V>builder().key(key).value(value).build();
  }

  /**
   * Creates a conditional put-operation for the given key and value with an expected-value so the
   * returned put-operation will check whether the current state in Nessie matches the expected
   * state in {@code expectedValue}.
   *
   * <p>Using a conditional put-operation for a contents-type that does not support global-state
   * results in an error.
   *
   * @param <V> the store value type
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @param expectedValue the expected value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  public static <V> Put<V> of(@Nonnull Key key, @Nonnull V value, @Nonnull V expectedValue) {
    return ImmutablePut.<V>builder().key(key).value(value).expectedValue(expectedValue).build();
  }
}

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
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;

/** Setting a new value. Can optionally declare whether the prior hash must match. */
@Value.Immutable
public interface Put extends Operation {

  /**
   * The value to store for this operation.
   *
   * @return the value
   */
  Content getValue();

  @Nullable
  @jakarta.annotation.Nullable
  Content getExpectedValue();

  /**
   * Creates a put-operation for the given key and value without an expected-value so the returned
   * put-operation is unconditional.
   *
   * <p>Unconditional put-operations must be used for content-types that do not support global-state
   * and for those that do support global-state when a new content object is added.
   *
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  static Put of(
      @Nonnull @jakarta.annotation.Nonnull ContentKey key,
      @Nonnull @jakarta.annotation.Nonnull Content value) {
    return ImmutablePut.builder().key(key).value(value).build();
  }

  /**
   * Creates a conditional put-operation for the given key and value with an expected-value so the
   * returned put-operation will check whether the current state in Nessie matches the expected
   * state in {@code expectedValue}.
   *
   * <p>Using a conditional put-operation for a content-type that does not support global-state
   * results in an error.
   *
   * @param key the key impacted by the operation
   * @param value the new value associated with the key
   * @param expectedValue the expected value associated with the key
   * @return a put operation for the key and value
   */
  @Nonnull
  @jakarta.annotation.Nonnull
  static Put of(
      @Nonnull @jakarta.annotation.Nonnull ContentKey key,
      @Nonnull @jakarta.annotation.Nonnull Content value,
      @Nonnull @jakarta.annotation.Nonnull Content expectedValue) {
    return ImmutablePut.builder().key(key).value(value).expectedValue(expectedValue).build();
  }
}

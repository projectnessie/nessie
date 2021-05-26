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

/**
 * A wrapper class that allows a value to be returned with an associated hash (a concrete ref).
 *
 * @param <T> The underlying value that will be returned.
 */
@Value.Immutable
public interface WithHash<T> {

  /** Get the value of the hash associated with this commit. */
  Hash getHash();

  /** Get the value this object wraps. */
  T getValue();

  /**
   * Build a WithHash object of type T.
   *
   * @param <T> The value type to hold
   * @param hash The hash this value type is connected to.
   * @param value The value held.
   * @return A new WithHash object.
   */
  public static <T> WithHash<T> of(Hash hash, T value) {
    return ImmutableWithHash.<T>builder().hash(hash).value(value).build();
  }
}

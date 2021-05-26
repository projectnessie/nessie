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
 * A wrapper class that allows a value to be returned with an associated payload.
 *
 * @param <T> The underlying value that will be returned.
 * @param <E> The enum defining the type.
 */
@Value.Immutable
public interface WithType<T, E extends Enum<E>> {

  /** Get the type of this entity as an enum. */
  Enum<E> getType();

  /** Get the value this object wraps. */
  T getValue();

  /**
   * Build a WithPayload object of type T.
   *
   * @param <T> The value type to hold
   * @param type The type this value is connected to
   * @param value The value held.
   * @return A new WithPayload object.
   */
  public static <T, E extends Enum<E>> WithType<T, E> of(E type, T value) {
    return ImmutableWithType.<T, E>builder().type(type).value(value).build();
  }
}

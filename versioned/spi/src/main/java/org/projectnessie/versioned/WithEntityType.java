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
 * A wrapper class that allows a value to be returned with an associated type (eg iceberg table, view).
 *
 * @param <T> The underlying value that will be returned.
 */
@Value.Immutable
public interface WithEntityType<T> {

  /**
   * Get the type associated with this value.
   */
  String getEntityType();

  /**
   * Get the value this object wraps.
   */
  T getEntity();

  /**
   * Build a WithType object of type T.
   * @param <T> The value type to hold
   * @param type The type this value is connected to.
   * @param value The value held.
   * @return A new WithType object.
   */
  public static <T> WithEntityType<T> of(String type, T value) {
    return ImmutableWithEntityType.<T>builder().entityType(type).entity(value).build();
  }
}

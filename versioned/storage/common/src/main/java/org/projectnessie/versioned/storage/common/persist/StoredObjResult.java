/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.common.persist;

import jakarta.annotation.Nullable;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
public interface StoredObjResult<T> {
  /**
   * Either contains the newly persisted or already existing object or is empty, if the object was
   * not persisted due to a {@link ObjId} collision.
   */
  @Value.Parameter
  Optional<T> obj();

  /**
   * Flag whether the object was persisted as a new object, {@link #obj()} will have a value if
   * {@code stored} is {@code true}.
   */
  @Value.Parameter
  boolean stored();

  static <T> StoredObjResult<T> storedObjResult(@Nullable T obj, boolean stored) {
    return ImmutableStoredObjResult.of(Optional.ofNullable(obj), stored);
  }
}

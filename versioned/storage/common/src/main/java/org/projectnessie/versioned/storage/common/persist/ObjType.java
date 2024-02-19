/*
 * Copyright (C) 2023 Dremio
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

import java.util.function.LongSupplier;

public interface ObjType {

  /** Must be unique among all registered object types. */
  String name();

  /** Must be unique among all registered object types. */
  String shortName();

  /** The target class that objects of this type should be serialized from and deserialized to. */
  Class<? extends Obj> targetClass();

  /**
   * Allows an object type to define how long a particular object instance can be cached.
   *
   * <p>{@value #CACHE_UNLIMITED}, which is the default implementation, defines that an object
   * instance can be cached forever.
   *
   * <p>{@value #NOT_CACHED} defines that an object instance must never be cached.
   *
   * <p>A positive value defines the timestamp in microseconds since epoch when the cached object
   * can be evicted
   */
  default long cachedObjectExpiresAtMicros(Obj obj, LongSupplier clock) {
    return CACHE_UNLIMITED;
  }

  /**
   * Allows an object type to define how long the fact of a non-existing object instance can be
   * cached.
   *
   * <p>{@value #CACHE_UNLIMITED} defines that an object instance can be cached forever.
   *
   * <p>{@value #NOT_CACHED}, which is the default implementation, defines that an object instance
   * must never be cached.
   *
   * <p>A positive value defines the timestamp in microseconds since epoch when the negative-cache
   * sentinel can be evicted
   */
  default long negativeCacheExpiresAtMicros(LongSupplier clock) {
    return NOT_CACHED;
  }

  long CACHE_UNLIMITED = -1L;
  long NOT_CACHED = 0L;
}

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
package org.projectnessie.versioned.storage.cache;

import jakarta.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

/** Cache primitives for a specific repository ID, used for one {@link Persist} instance. */
public interface ObjCache {

  /**
   * Returns the {@link Obj} for the given {@link ObjId id}.
   *
   * @return One of these alternatives: the cached object if present, the {@link
   *     CacheBackend#NOT_FOUND_OBJ_SENTINEL} indicating that the object does <em>not</em> exist as
   *     previously marked via {@link #putReferenceNegative(ObjId, ObjType)}, or {@code null}.
   */
  Obj get(@Nonnull ObjId id);

  /**
   * Adds the given object to the local cache and sends a cache-invalidation message to Nessie
   * peers.
   */
  void put(@Nonnull Obj obj);

  /** Adds the given object only to the local cache, does not send a cache-invalidation message. */
  void putLocal(@Nonnull Obj obj);

  /**
   * Record the "not found" sentinel for the given {@link ObjId id} and {@link ObjType type}.
   * Behaves like {@link #remove(ObjId)}, if {@code type} is {@code null}.
   */
  void putReferenceNegative(ObjId id, ObjType type);

  void remove(@Nonnull ObjId id);

  void clear();

  Reference getReference(@Nonnull String name);

  void removeReference(@Nonnull String name);

  /**
   * Adds the given reference to the local cache and sends a cache-invalidation message to Nessie
   * peers.
   */
  void putReference(@Nonnull Reference r);

  /**
   * Adds the given reference only to the local cache, does not send a cache-invalidation message.
   */
  void putReferenceLocal(@Nonnull Reference r);

  void putReferenceNegative(@Nonnull String name);
}

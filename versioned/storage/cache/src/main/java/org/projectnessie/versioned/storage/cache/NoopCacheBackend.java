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
package org.projectnessie.versioned.storage.cache;

import jakarta.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;

final class NoopCacheBackend implements CacheBackend {

  static final NoopCacheBackend INSTANCE = new NoopCacheBackend();

  private NoopCacheBackend() {}

  @Override
  public Persist wrap(@Nonnull Persist perist) {
    return perist;
  }

  @Override
  public void putReferenceNegative(@Nonnull String repositoryId, @Nonnull String name) {}

  @Override
  public void putReference(@Nonnull String repositoryId, @Nonnull Reference r) {}

  @Override
  public void putReferenceLocal(@Nonnull String repositoryId, @Nonnull Reference r) {}

  @Override
  public void removeReference(@Nonnull String repositoryId, @Nonnull String name) {}

  @Override
  public Reference getReference(@Nonnull String repositoryId, @Nonnull String name) {
    return null;
  }

  @Override
  public void clear(@Nonnull String repositoryId) {}

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjId id) {}

  @Override
  public void putNegative(@Nonnull String repositoryId, @Nonnull ObjId id, @Nonnull ObjType type) {}

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {}

  @Override
  public void putLocal(@Nonnull String repositoryId, @Nonnull Obj obj) {}

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id) {
    return null;
  }
}

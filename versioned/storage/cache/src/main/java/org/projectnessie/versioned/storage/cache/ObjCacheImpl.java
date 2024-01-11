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
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class ObjCacheImpl implements ObjCache {
  private final CacheBackend backend;
  private final String repositoryId;

  ObjCacheImpl(CacheBackend backend, StoreConfig config) {
    this.backend = backend;
    this.repositoryId = config.repositoryId();
  }

  @Override
  public Obj get(@Nonnull ObjId id) {
    return backend.get(repositoryId, id);
  }

  @Override
  public void put(@Nonnull Obj obj) {
    backend.put(repositoryId, obj);
  }

  @Override
  public void remove(@Nonnull ObjId id) {
    backend.remove(repositoryId, id);
  }

  @Override
  public void clear() {
    backend.clear(repositoryId);
  }
}

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
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;

final class DistributedInvalidationsCacheBackend implements CacheBackend {
  private final CacheBackend local;
  private final DistributedCacheInvalidation sender;

  DistributedInvalidationsCacheBackend(
      DistributedCacheInvalidations distributedCacheInvalidations) {
    this.local = distributedCacheInvalidations.localBackend();
    this.sender = distributedCacheInvalidations.invalidationSender();
    distributedCacheInvalidations
        .invalidationListenerReceiver()
        .accept(
            new DistributedCacheInvalidation() {
              @Override
              public void removeObj(String repositoryId, ObjId objId) {
                local.remove(repositoryId, objId);
              }

              @Override
              public void putObj(String repositoryId, ObjId objId, int hash) {
                Obj existing = local.get(repositoryId, objId);
                if (existing != null && existing.hashCode() != hash) {
                  local.remove(repositoryId, objId);
                }
              }

              @Override
              public void removeReference(String repositoryId, String refName) {
                local.removeReference(repositoryId, refName);
              }

              @Override
              public void putReference(String repositoryId, String refName, int hash) {
                Reference existing = local.getReference(repositoryId, refName);
                if (existing != null && existing.hashCode() != hash) {
                  local.removeReference(repositoryId, refName);
                }
              }
            });
  }

  @Override
  public Persist wrap(@Nonnull Persist persist) {
    ObjCacheImpl cache = new ObjCacheImpl(this, persist.config());
    return new CachingPersistImpl(persist, cache);
  }

  @Override
  public Obj get(@Nonnull String repositoryId, @Nonnull ObjId id) {
    return local.get(repositoryId, id);
  }

  @Override
  public void put(@Nonnull String repositoryId, @Nonnull Obj obj) {
    local.put(repositoryId, obj);
    if (obj instanceof UpdateableObj) {
      sender.putObj(repositoryId, obj.id(), obj.hashCode());
    }
  }

  @Override
  public void remove(@Nonnull String repositoryId, @Nonnull ObjId id) {
    local.remove(repositoryId, id);
    sender.removeObj(repositoryId, id);
  }

  @Override
  public void clear(@Nonnull String repositoryId) {
    local.clear(repositoryId);
  }

  @Override
  public Reference getReference(@Nonnull String repositoryId, @Nonnull String name) {
    return local.getReference(repositoryId, name);
  }

  @Override
  public void removeReference(@Nonnull String repositoryId, @Nonnull String name) {
    local.removeReference(repositoryId, name);
    sender.removeReference(repositoryId, name);
  }

  @Override
  public void putReference(@Nonnull String repositoryId, @Nonnull Reference r) {
    local.putReference(repositoryId, r);
    sender.putReference(repositoryId, r.name(), r.hashCode());
  }

  @Override
  public void putNegative(@Nonnull String repositoryId, @Nonnull String name) {
    local.putNegative(repositoryId, name);
  }
}

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

import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.serialize.ProtoSerialization.serializeObj;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.serialize.ProtoSerialization;

@Value.Immutable
abstract class CaffeineCacheBackend implements CacheBackend {

  public static final int JAVA_OBJ_HEADER = 32;
  public static final int JAVA_INT = 4;
  public static final int JAVA_LONG = 8;
  private final Map<String, Integer> repositoryIdMap = new ConcurrentHashMap<>();
  private final AtomicInteger repositoryKey = new AtomicInteger();

  static ImmutableCaffeineCacheBackend.Builder builder() {
    return ImmutableCaffeineCacheBackend.builder();
  }

  abstract long capacity();

  @Value.Derived
  Cache<CacheKey, byte[]> cache() {
    // IMPORTANT!
    // When changing the configuration of the Caffeine cache, make sure to run the
    // _native_ Quarkus tests and adopt the `@ReflectionConfig` in
    // org.projectnessie.quarkus.providers.PersistProvider.
    return Caffeine.newBuilder()
        .maximumWeight(capacity())
        .recordStats()
        .weigher(this::weigher)
        .build();
  }

  @Override
  public Persist wrap(@Nonnull @jakarta.annotation.Nonnull Persist persist) {
    ObjCacheImpl cache = new ObjCacheImpl(this, persist.config().repositoryId());
    return new CachingPersistImpl(persist, cache);
  }

  private int weigher(CacheKey key, byte[] data) {
    return key.heapSize() + JAVA_OBJ_HEADER + data.length;
  }

  @Override
  public Obj get(
      @Nonnull @jakarta.annotation.Nonnull String repositoryId,
      @Nonnull @jakarta.annotation.Nonnull ObjId id) {
    if (EMPTY_OBJ_ID.equals(id)) {
      return null;
    }
    CacheKey key = cacheKey(repositoryId, id);
    byte[] bytes = cache().getIfPresent(key);
    return bytes != null ? ProtoSerialization.deserializeObj(id, bytes) : null;
  }

  @Override
  public void put(
      @Nonnull @jakarta.annotation.Nonnull String repositoryId,
      @Nonnull @jakarta.annotation.Nonnull Obj obj) {
    if (EMPTY_OBJ_ID.equals(obj.id())) {
      return;
    }
    CacheKey key = cacheKey(repositoryId, obj.id());
    try {
      cache().put(key, serializeObj(obj, Integer.MAX_VALUE, Integer.MAX_VALUE));
    } catch (ObjTooLargeException e) {
      // this should never happen
      throw new RuntimeException(e);
    }
  }

  @Override
  public void remove(
      @Nonnull @jakarta.annotation.Nonnull String repositoryId,
      @Nonnull @jakarta.annotation.Nonnull ObjId id) {
    if (EMPTY_OBJ_ID.equals(id)) {
      return;
    }
    CacheKey key = cacheKey(repositoryId, id);
    cache().invalidate(key);
  }

  @Override
  public void clear(@Nonnull @jakarta.annotation.Nonnull String repositoryId) {
    // Do not actually remove the cached elements, but instead use a different base-cache-key for
    // the repository, so old cached entries will not be found.
    repositoryIdMap.computeIfPresent(repositoryId, (k, v) -> repositoryKey.incrementAndGet());
  }

  private CacheKey cacheKey(String repositoryId, ObjId id) {
    return new CacheKey(repositoryKey(repositoryId), id);
  }

  private int repositoryKey(String repositoryId) {
    return repositoryIdMap.computeIfAbsent(repositoryId, k -> repositoryKey.incrementAndGet());
  }

  static final class CacheKey {

    public static final int HEAP_OVERHEAD =
        JAVA_OBJ_HEADER + JAVA_INT + JAVA_LONG + JAVA_OBJ_HEADER;
    final int repoKey;
    final ObjId id;

    CacheKey(int repoKey, ObjId id) {
      this.repoKey = repoKey;
      this.id = id;
    }

    int heapSize() {
      return HEAP_OVERHEAD + id.size();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheKey)) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return repoKey == cacheKey.repoKey && id.equals(cacheKey.id);
    }

    @Override
    public int hashCode() {
      return repoKey * 31 + id.hashCode();
    }

    @Override
    public String toString() {
      return "CacheKey{" + repoKey + ", " + id + '}';
    }
  }
}

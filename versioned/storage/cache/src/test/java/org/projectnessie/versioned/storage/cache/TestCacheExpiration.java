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

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheExpiration {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void cachingObjectsExpiration(boolean enableSoftReferences) {
    AtomicLong currentTime = new AtomicLong(1234L);

    CaffeineCacheBackend backend =
        new CaffeineCacheBackend(
            CacheConfig.builder()
                .capacityMb(8)
                .clockNanos(() -> MICROSECONDS.toNanos(currentTime.get()))
                .enableSoftReferences(enableSoftReferences)
                .cacheCapacityOvershoot(0.1d)
                .build());

    CacheTestObjTypeBundle.DefaultCachingObj defaultCachingObj =
        ImmutableDefaultCachingObj.builder().id(randomObjId()).value("def").build();
    CacheTestObjTypeBundle.NonCachingObj nonCachingObj =
        ImmutableNonCachingObj.builder().id(randomObjId()).value("foo").build();
    CacheTestObjTypeBundle.DynamicCachingObj dynamicCachingObj =
        ImmutableDynamicCachingObj.builder().id(randomObjId()).thatExpireTimestamp(2L).build();
    ContentValueObj stdObj = contentValue("cid", 42, ByteString.EMPTY);

    backend.put("repo", defaultCachingObj);
    backend.put("repo", nonCachingObj);
    backend.put("repo", dynamicCachingObj);
    backend.put("repo", stdObj);

    ConcurrentMap<CaffeineCacheBackend.CacheKeyValue, CaffeineCacheBackend.CacheKeyValue> cacheMap =
        backend.cache.asMap();

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKeyForRead("repo", nonCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", dynamicCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", defaultCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", stdObj.id()))
        .hasSize(3);

    soft.assertThat(backend.get("repo", nonCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", dynamicCachingObj.id())).isEqualTo(dynamicCachingObj);
    soft.assertThat(backend.get("repo", defaultCachingObj.id())).isEqualTo(defaultCachingObj);
    soft.assertThat(backend.get("repo", stdObj.id())).isEqualTo(stdObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKeyForRead("repo", nonCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", dynamicCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", defaultCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", stdObj.id()))
        .hasSize(3);

    // increment clock by one - "dynamic" object should still be present

    currentTime.addAndGet(1);

    soft.assertThat(backend.get("repo", nonCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", dynamicCachingObj.id())).isEqualTo(dynamicCachingObj);
    soft.assertThat(backend.get("repo", defaultCachingObj.id())).isEqualTo(defaultCachingObj);
    soft.assertThat(backend.get("repo", stdObj.id())).isEqualTo(stdObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKeyForRead("repo", nonCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", dynamicCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", defaultCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", stdObj.id()))
        .hasSize(3);

    // increment clock by one again - "dynamic" object should go away

    currentTime.addAndGet(1);

    soft.assertThat(backend.get("repo", nonCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", dynamicCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", defaultCachingObj.id())).isEqualTo(defaultCachingObj);
    soft.assertThat(backend.get("repo", stdObj.id())).isEqualTo(stdObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKeyForRead("repo", nonCachingObj.id()))
        .doesNotContainKey(CaffeineCacheBackend.cacheKeyForRead("repo", dynamicCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", defaultCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKeyForRead("repo", stdObj.id()));
    // note: Caffeine's cache-map incorrectly reports a size of 3 here, although the map itself only
    // returns the only left object
  }
}

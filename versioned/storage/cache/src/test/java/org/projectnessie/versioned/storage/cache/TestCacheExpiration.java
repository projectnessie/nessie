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
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheExpiration {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void cachingObjectsExpiration() {
    AtomicLong currentTime = new AtomicLong(1234L);

    CaffeineCacheBackend backend =
        CaffeineCacheBackend.builder()
            .capacity(8)
            .clockNanos(() -> MICROSECONDS.toNanos(currentTime.get()))
            .build();

    CacheTestObjTypeBundle.NonCachingObj nonCachingObj =
        ImmutableNonCachingObj.builder().id(randomObjId()).value("foo").build();
    CacheTestObjTypeBundle.DynamicCachingObj dynamicCachingObj =
        ImmutableDynamicCachingObj.builder().id(randomObjId()).thatExpireTimestamp(2L).build();

    backend.put("repo", nonCachingObj);
    backend.put("repo", dynamicCachingObj);

    ConcurrentMap<CaffeineCacheBackend.CacheKeyValue, byte[]> cacheMap = backend.cache().asMap();

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKey("repo", nonCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKey("repo", dynamicCachingObj.id()))
        .hasSize(1);

    soft.assertThat(backend.get("repo", nonCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", dynamicCachingObj.id())).isEqualTo(dynamicCachingObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKey("repo", nonCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKey("repo", dynamicCachingObj.id()))
        .hasSize(1);

    // increment clock by one - "dynamic" object should still be present

    currentTime.addAndGet(1);

    soft.assertThat(backend.get("repo", nonCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", dynamicCachingObj.id())).isEqualTo(dynamicCachingObj);

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKey("repo", nonCachingObj.id()))
        .containsKey(CaffeineCacheBackend.cacheKey("repo", dynamicCachingObj.id()))
        .hasSize(1);

    // increment clock by one again - "dynamic" object should go away

    currentTime.addAndGet(1);

    soft.assertThat(backend.get("repo", nonCachingObj.id())).isNull();
    soft.assertThat(backend.get("repo", dynamicCachingObj.id())).isNull();

    soft.assertThat(cacheMap)
        .doesNotContainKey(CaffeineCacheBackend.cacheKey("repo", nonCachingObj.id()))
        .doesNotContainKey(CaffeineCacheBackend.cacheKey("repo", dynamicCachingObj.id()));
  }
}

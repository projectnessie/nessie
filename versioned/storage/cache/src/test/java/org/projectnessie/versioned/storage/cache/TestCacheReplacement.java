/*
 * Copyright (C) 2026 Dremio
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.storage.cache.CacheBackend.NON_EXISTENT_REFERENCE_SENTINEL;
import static org.projectnessie.versioned.storage.cache.CacheBackend.NOT_FOUND_OBJ_SENTINEL;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.ObjType.CACHE_UNLIMITED;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.storage.cache.CacheTestObjTypeBundle.NegativeCachingObj;

class TestCacheReplacement {

  @Test
  void replacementRetainsOnlyCurrentPayload() throws IllegalAccessException {
    var backend =
        new CaffeineCacheBackend(
            CacheConfig.builder().capacityMb(8).cacheCapacityOvershoot(0.1d).build());
    var id = randomObjId();
    long previousWeight = 0;
    for (int size : new int[] {16384, 32768, 8192}) {
      var obj = ImmutableDefaultCachingObj.builder().id(id).value("a".repeat(size)).build();
      backend.putLocal("repo", obj);
      backend.cache.cleanUp();

      assertThat(backend.get("repo", id)).isEqualTo(obj);
      assertThat(backend.cache.asMap()).hasSize(1);
      var entry = backend.cache.asMap().entrySet().iterator().next();
      var retainedPayloads = Collections.newSetFromMap(new IdentityHashMap<byte[], Boolean>());
      // Inspect both retained instances: weightedSize alone cannot detect payloads kept by keys.
      for (var holder : List.of(entry.getKey(), entry.getValue())) {
        for (var field : holder.getClass().getDeclaredFields()) {
          if (field.getType() == byte[].class) {
            var payload = (byte[]) field.get(holder);
            if (payload != null) {
              retainedPayloads.add(payload);
            }
          }
        }
      }
      assertThat(retainedPayloads).hasSize(1);
      assertThat(backend.currentWeightReported())
          .isGreaterThanOrEqualTo(retainedPayloads.iterator().next().length);
      if (size == 32768) {
        assertThat(backend.currentWeightReported()).isGreaterThan(previousWeight);
      } else if (size == 8192) {
        assertThat(backend.currentWeightReported()).isLessThan(previousWeight);
      }
      previousWeight = backend.currentWeightReported();
    }
  }

  @Test
  void replacementUsesCurrentObjectExpiration() {
    var now = new AtomicLong(1000);
    var backend = newBackend(now);
    var id = randomObjId();
    var first = ImmutableDynamicCachingObj.builder().id(id).thatExpireTimestamp(10).build();
    backend.putLocal("repo", first);

    now.set(6000);
    var replacement = ImmutableDynamicCachingObj.builder().id(id).thatExpireTimestamp(20).build();
    backend.putLocal("repo", replacement);
    now.set(12000);
    assertThat(backend.get("repo", id)).isEqualTo(replacement);
    now.set(26000);
    assertThat(backend.get("repo", id)).isNull();

    // A new short-lived value must also shorten the previous value's deadline.
    now.set(30000);
    backend.putLocal("repo", replacement);
    now.set(31000);
    backend.putLocal("repo", first);
    now.set(41000);
    assertThat(backend.get("repo", id)).isNull();
  }

  @Test
  void replacementBetweenPositiveAndNegativeObjects() {
    var backend = newBackend(new AtomicLong(1000));
    var id = randomObjId();
    var obj = ImmutableNegativeCachingObj.builder().id(id).build();
    backend.putNegative("repo", id, NegativeCachingObj.TYPE);
    assertThat(backend.get("repo", id)).isSameAs(NOT_FOUND_OBJ_SENTINEL);
    backend.putLocal("repo", obj);
    assertThat(backend.get("repo", id)).isEqualTo(obj);
    backend.cache.cleanUp();
    var positiveWeight = backend.currentWeightReported();
    backend.putNegative("repo", id, NegativeCachingObj.TYPE);
    assertThat(backend.get("repo", id)).isSameAs(NOT_FOUND_OBJ_SENTINEL);
    backend.cache.cleanUp();
    assertThat(backend.currentWeightReported()).isLessThan(positiveWeight);
    assertThat(backend.cache.asMap().values())
        .singleElement()
        .satisfies(value -> assertThat(value.expiresAtNanosEpoch).isEqualTo(CACHE_UNLIMITED));
  }

  @Test
  void referenceReplacementUsesCurrentPayloadAndExpiration() {
    var now = new AtomicLong(1000);
    var backend = newBackend(now);
    var first = reference("refs/heads/test", randomObjId(), false, 1L, null);
    var replacement = reference(first.name(), randomObjId(), false, 2L, null);
    backend.putReferenceLocal("repo", first);
    now.addAndGet(Duration.ofSeconds(30).toNanos());
    backend.putReferenceLocal("repo", replacement);
    now.addAndGet(Duration.ofSeconds(31).toNanos());
    assertThat(backend.getReference("repo", first.name())).isEqualTo(replacement);
    backend.putReferenceNegative("repo", first.name());
    assertThat(backend.getReference("repo", first.name()))
        .isSameAs(NON_EXISTENT_REFERENCE_SENTINEL);
    now.addAndGet(Duration.ofSeconds(1).toNanos());
    assertThat(backend.getReference("repo", first.name())).isNull();

    backend.putReferenceNegative("repo", first.name());
    backend.putReferenceLocal("repo", replacement);
    now.addAndGet(Duration.ofSeconds(2).toNanos());
    assertThat(backend.getReference("repo", first.name())).isEqualTo(replacement);
    now.addAndGet(Duration.ofMinutes(1).toNanos());
    assertThat(backend.getReference("repo", first.name())).isNull();
  }

  @Test
  void replacementIsIsolatedByRepository() {
    var backend = newBackend(new AtomicLong(1000));
    var id = randomObjId();
    var first = ImmutableDefaultCachingObj.builder().id(id).value("first").build();
    var replacement = ImmutableDefaultCachingObj.builder().id(id).value("replacement").build();
    backend.putLocal("repo1", first);
    backend.putLocal("repo2", first);
    backend.putLocal("repo1", replacement);
    assertThat(backend.get("repo1", id)).isEqualTo(replacement);
    assertThat(backend.get("repo2", id)).isEqualTo(first);
    backend.clear("repo1");
    assertThat(backend.get("repo1", id)).isNull();
    assertThat(backend.get("repo2", id)).isEqualTo(first);
    backend.remove("repo2", id);
    assertThat(backend.get("repo2", id)).isNull();
  }

  private static CaffeineCacheBackend newBackend(AtomicLong now) {
    return new CaffeineCacheBackend(
        CacheConfig.builder()
            .capacityMb(8)
            .cacheCapacityOvershoot(0.1d)
            .clockNanos(now::get)
            .referenceTtl(Duration.ofMinutes(1))
            .referenceNegativeTtl(Duration.ofSeconds(1))
            .build());
  }
}

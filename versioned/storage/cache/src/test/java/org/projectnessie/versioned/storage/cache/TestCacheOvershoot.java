/*
 * Copyright (C) 2025 Dremio
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
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import com.google.common.base.Strings;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.commontests.objtypes.SimpleTestObj;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCacheOvershoot {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void testCacheOvershoot() throws Exception {
    var config = CacheConfig.builder().capacityMb(4).cacheCapacityOvershoot(0.1d).build();
    var cache = new CaffeineCacheBackend(config);

    var maxWeight = config.capacityMb() * 1024L * 1024L;
    var limitWeight = maxWeight + (long) (maxWeight * config.cacheCapacityOvershoot());

    var str = Strings.repeat("a", 4096);

    cache.put("repo", SimpleTestObj.builder().id(randomObjId()).text(str).build());
    var objWeight = cache.currentWeightTracked();

    while (cache.currentWeightTracked() < maxWeight - objWeight) {
      cache.put("repo", SimpleTestObj.builder().id(randomObjId()).text(str).build());
    }

    soft.assertThat(cache.currentWeightTracked()).isLessThanOrEqualTo(maxWeight);
    soft.assertThat(cache.rejections()).isEqualTo(0L);

    var numThreads = 8;
    var executor = Executors.newFixedThreadPool(numThreads);
    var seenRejections = false;
    var seenOvershoot = false;
    var seenLimitExceeded = false;
    var stop = new AtomicBoolean();
    try {
      for (int i = 0; i < numThreads; i++) {
        executor.execute(
            () -> {
              while (!stop.get()) {
                cache.put("repo", SimpleTestObj.builder().id(randomObjId()).text(str).build());
                Thread.yield();
              }
            });
      }

      for (int i = 0; i < 10000 && (!seenRejections || !seenOvershoot); i++) {
        Thread.sleep(10);
        if (cache.rejections() > 0) {
          seenRejections = true;
        }
        var w = cache.currentWeightTracked();
        if (w > maxWeight) {
          seenOvershoot = true;
          if (w > limitWeight) {
            seenLimitExceeded = true;
          }
        }
        assertThat(w).isGreaterThanOrEqualTo(0L);
      }

    } finally {
      stop.set(true);

      executor.shutdown();
      executor.awaitTermination(10, TimeUnit.MINUTES);
    }

    soft.assertThat(cache.currentWeightTracked()).isLessThanOrEqualTo(limitWeight);
    soft.assertThat(cache.rejections()).isGreaterThan(0L);
    soft.assertThat(seenRejections).isTrue();
    soft.assertThat(seenOvershoot).isTrue();
    soft.assertThat(seenLimitExceeded).isFalse();
  }
}

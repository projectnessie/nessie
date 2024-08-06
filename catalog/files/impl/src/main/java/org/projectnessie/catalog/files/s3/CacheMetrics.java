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
package org.projectnessie.catalog.files.s3;

import static java.util.Collections.singletonList;

import com.github.benmanes.caffeine.cache.stats.StatsCounter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;
import java.util.Optional;

final class CacheMetrics {

  static StatsCounter statsCounter(
      Optional<MeterRegistry> meterRegistry, String cacheName, int maxSize) {
    if (meterRegistry.isPresent()) {
      meterRegistry
          .get()
          .gauge("max_entries", singletonList(Tag.of("cache", cacheName)), "", x -> maxSize);

      return new CaffeineStatsCounter(meterRegistry.get(), cacheName);
    }
    return StatsCounter.disabledStatsCounter();
  }
}

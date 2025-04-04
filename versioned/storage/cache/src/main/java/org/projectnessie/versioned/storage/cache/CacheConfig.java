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

import static com.google.common.base.Preconditions.checkState;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.LongSupplier;
import org.immutables.value.Value;

@Value.Immutable
public interface CacheConfig {

  String INVALID_REFERENCE_NEGATIVE_TTL =
      "Cache reference-negative-TTL must only be present, if reference-TTL is configured, and must only be positive.";
  String INVALID_REFERENCE_TTL = "Cache reference-TTL must be positive, if present.";

  long capacityMb();

  Optional<MeterRegistry> meterRegistry();

  Optional<Duration> referenceTtl();

  Optional<Duration> referenceNegativeTtl();

  Optional<Boolean> enableSoftReferences();

  double cacheCapacityOvershoot();

  @Value.Default
  default LongSupplier clockNanos() {
    return System::nanoTime;
  }

  /**
   * Executor used by Caffeine, see {@link
   * com.github.benmanes.caffeine.cache.Caffeine#executor(Executor)}, only present for targeted
   * tests.
   */
  @Value.Default
  default Executor executor() {
    return Runnable::run;
  }

  static Builder builder() {
    return ImmutableCacheConfig.builder();
  }

  @Value.Check
  default void check() {
    referenceTtl()
        .ifPresent(ttl -> checkState(ttl.compareTo(Duration.ZERO) > 0, INVALID_REFERENCE_TTL));
    referenceNegativeTtl()
        .ifPresent(
            ttl ->
                checkState(
                    referenceTtl().isPresent() && ttl.compareTo(Duration.ZERO) > 0,
                    INVALID_REFERENCE_NEGATIVE_TTL));
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder capacityMb(long capacityMb);

    @CanIgnoreReturnValue
    Builder meterRegistry(MeterRegistry meterRegistry);

    @CanIgnoreReturnValue
    Builder referenceTtl(Duration referenceTtl);

    @CanIgnoreReturnValue
    Builder referenceNegativeTtl(Duration referenceNegativeTtl);

    @CanIgnoreReturnValue
    Builder clockNanos(LongSupplier clockNanos);

    @CanIgnoreReturnValue
    Builder enableSoftReferences(boolean enableSoftReferences);

    @CanIgnoreReturnValue
    Builder cacheCapacityOvershoot(double cacheCapacityOvershoot);

    @CanIgnoreReturnValue
    Builder executor(Executor executor);

    CacheConfig build();
  }
}

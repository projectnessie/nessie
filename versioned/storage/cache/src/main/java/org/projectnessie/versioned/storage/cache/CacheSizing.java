/*
 * Copyright (C) 2023 Dremio
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
import static org.projectnessie.versioned.storage.common.config.StoreConfig.DEFAULT_CACHE_CAPACITY_FRACTION_ADJUST_MB;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.DEFAULT_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.DEFAULT_CACHE_CAPACITY_FRACTION_OF_HEAP;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
public interface CacheSizing {

  int DEFAULT_HEAP_SIZE_KEEP_FREE = DEFAULT_CACHE_CAPACITY_FRACTION_ADJUST_MB;
  int DEFAULT_MIN_SIZE_MB = DEFAULT_CACHE_CAPACITY_FRACTION_MIN_SIZE_MB;
  double DEFAULT_HEAP_FRACTION = DEFAULT_CACHE_CAPACITY_FRACTION_OF_HEAP;

  OptionalInt fixedSizeInMB();

  OptionalDouble fractionOfMaxHeapSize();

  OptionalInt fractionMinSizeMb();

  OptionalInt heapSizeAdjustmentMB();

  @Value.Derived
  default int effectiveSizeInMB() {
    return calculateEffectiveSizeInMB(Runtime.getRuntime().maxMemory());
  }

  default int calculateEffectiveSizeInMB(long maxHeapInBytes) {
    if (fixedSizeInMB().isPresent()) {
      return fixedSizeInMB().getAsInt();
    }

    long fractionAsBytes =
        (long) (fractionOfMaxHeapSize().orElse(DEFAULT_HEAP_FRACTION) * maxHeapInBytes);

    long freeHeap = maxHeapInBytes - fractionAsBytes;
    long minFree = heapSizeAdjustmentMB().orElse(DEFAULT_HEAP_SIZE_KEEP_FREE) * 1024L * 1024L;

    long capacityInBytes = (minFree > freeHeap) ? maxHeapInBytes - minFree : fractionAsBytes;

    long fractionMin = (long) fractionMinSizeMb().orElse(DEFAULT_MIN_SIZE_MB) * 1024 * 1024;
    if (capacityInBytes < fractionMin) {
      capacityInBytes = fractionMin;
    }

    return (int) (capacityInBytes / 1024L / 1024L);
  }

  static Builder builder() {
    return ImmutableCacheSizing.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder fixedSizeInMB(int fixedSizeInMB);

    @CanIgnoreReturnValue
    Builder fixedSizeInMB(OptionalInt fixedSizeInMB);

    @CanIgnoreReturnValue
    Builder fractionOfMaxHeapSize(double fractionOfMaxHeapSize);

    @CanIgnoreReturnValue
    Builder fractionOfMaxHeapSize(OptionalDouble fractionOfMaxHeapSize);

    @CanIgnoreReturnValue
    Builder fractionMinSizeMb(int fractionMinSizeMb);

    @CanIgnoreReturnValue
    Builder fractionMinSizeMb(OptionalInt fractionMinSizeMb);

    @CanIgnoreReturnValue
    Builder heapSizeAdjustmentMB(int heapSizeAdjustmentMB);

    @CanIgnoreReturnValue
    Builder heapSizeAdjustmentMB(OptionalInt heapSizeAdjustmentMB);

    CacheSizing build();
  }

  @Value.Check
  default void check() {
    if (fractionOfMaxHeapSize().isPresent()) {
      checkState(
          fractionOfMaxHeapSize().getAsDouble() > 0d && fractionOfMaxHeapSize().getAsDouble() < 1d,
          "Cache sizing: fractionOfMaxHeapSize must be > 0 and < 1, but is %s",
          fractionOfMaxHeapSize());
    }
    if (fixedSizeInMB().isPresent()) {
      int fixed = fixedSizeInMB().getAsInt();
      checkState(
          fixed >= 0,
          "Cache sizing: sizeInBytes must be greater than 0, but is %s",
          fixedSizeInMB());
    }
    checkState(
        heapSizeAdjustmentMB().orElse(DEFAULT_HEAP_SIZE_KEEP_FREE) > 64,
        "Cache sizing: heapSizeAdjustment must be greater than 64 MB, but is %s",
        heapSizeAdjustmentMB());
  }
}

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
package org.projectnessie.versioned.storage.cleanup;

import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_ATOMIC_LONG_ARRAY;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_BIT_ARRAY;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_BLOOM_FILTER;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_LONG_ADDER;
import static org.projectnessie.versioned.storage.cleanup.HeapSizes.HEAP_SIZE_PRIMITIVE_LONG_ARRAY;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.PrimitiveSink;
import java.util.concurrent.atomic.AtomicLong;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@SuppressWarnings("UnstableApiUsage")
final class ReferencedObjectsFilterImpl implements ReferencedObjectsFilter {

  private final BloomFilter<ObjId> filter;
  private final double allowedFalsePositiveProbability;
  private final AtomicLong remainingElements;
  private final long estimatedHeapPressure;

  ReferencedObjectsFilterImpl(CleanupParams params) {
    this.filter = createBloomFilter(params);
    this.remainingElements = new AtomicLong(params.expectedObjCount());
    this.allowedFalsePositiveProbability = params.allowedFalsePositiveProbability();
    this.estimatedHeapPressure = calculateEstimatedHeapPressure(params);
  }

  static BloomFilter<ObjId> createBloomFilter(CleanupParams params) {
    return BloomFilter.create(
        ReferencedObjectsFilterImpl::funnel,
        params.expectedObjCount(),
        params.falsePositiveProbability());
  }

  private static void funnel(ObjId id, PrimitiveSink primitiveSink) {
    var idSize = id.size();
    var i = 0;
    for (; idSize >= 8; idSize -= 8) {
      primitiveSink.putLong(id.longAt(i++));
    }
    i <<= 3;
    for (; idSize > 0; idSize--) {
      primitiveSink.putByte(id.byteAt(i++));
    }
  }

  @Override
  public boolean markReferenced(ObjId objId) {
    if (filter.put(objId)) {
      if (remainingElements.decrementAndGet() >= 0L || withinExpectedFpp()) {
        return true;
      }
      throw new MustRestartWithBiggerFilterRuntimeException(
          "Bloom filter exceeded the configured expected FPP");
    }
    return false;
  }

  @Override
  public boolean isProbablyReferenced(ObjId objId) {
    return filter.mightContain(objId);
  }

  @Override
  public boolean withinExpectedFpp() {
    return expectedFpp() <= allowedFalsePositiveProbability;
  }

  @Override
  public long approximateElementCount() {
    return filter.approximateElementCount();
  }

  @Override
  public double expectedFpp() {
    return filter.expectedFpp();
  }

  @Override
  public long estimatedHeapPressure() {
    return estimatedHeapPressure;
  }

  private static long calculateEstimatedHeapPressure(CleanupParams params) {
    var bits = optimalNumOfBits(params.expectedObjCount(), params.falsePositiveProbability());
    var arrayLen = bits / 64 + 1;
    return HEAP_SIZE_BLOOM_FILTER
        + HEAP_SIZE_BIT_ARRAY
        + HEAP_SIZE_LONG_ADDER
        + HEAP_SIZE_ATOMIC_LONG_ARRAY
        + HEAP_SIZE_PRIMITIVE_LONG_ARRAY * arrayLen;
  }

  // See com.google.common.hash.BloomFilter.optimalNumOfBits
  private static long optimalNumOfBits(long expectedInsertions, double fpp) {
    if (fpp == 0) {
      fpp = Double.MIN_VALUE;
    }
    return (long) (-expectedInsertions * Math.log(fpp) / (Math.log(2) * Math.log(2)));
  }
}

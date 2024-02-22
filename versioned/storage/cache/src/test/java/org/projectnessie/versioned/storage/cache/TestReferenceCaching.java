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

import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestReferenceCaching {
  public static final String REF_NAME = "refs/heads/foo";
  @InjectSoftAssertions protected SoftAssertions soft;

  Persist wrapWithCache(Persist persist, LongSupplier clockNanos) {
    return PersistCaches.newBackend(
            CacheConfig.builder()
                .capacityMb(32)
                .clockNanos(clockNanos)
                .referenceTtl(Duration.ofMinutes(1))
                .referenceNegativeTtl(Duration.ofSeconds(1))
                .build())
        .wrap(persist);
  }

  // Two caching `Persist` instances, using _independent_ cache backends.
  Persist withCache1;
  Persist withCache2;

  AtomicLong nowNanos;

  @BeforeEach
  void wrapCaches(@NessiePersist Persist persist1, @NessiePersist Persist persist2) {
    nowNanos = new AtomicLong();
    withCache1 = wrapWithCache(persist1, nowNanos::get);
    withCache2 = wrapWithCache(persist2, nowNanos::get);
  }

  /** Explicit cache-expiry via {@link Persist#fetchReferenceForUpdate(String)}. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void referenceCacheInconsistency(boolean bulk) throws Exception {
    // Create ref via instance 1
    Reference ref =
        reference(REF_NAME, randomObjId(), false, withCache1.config().currentTimeMicros(), null);
    withCache1.addReference(ref);

    // Populate cache in instance 2
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(ref);

    // Update ref via instance 1
    Reference refUpdated = withCache1.updateReferencePointer(ref, randomObjId());
    soft.assertThat(refUpdated).isNotEqualTo(ref);

    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isEqualTo(refUpdated);
    // Other test instance did NOT update its cache
    soft.assertThat(fetchRef(withCache2, bulk, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refUpdated.pointer())
        .isEqualTo(ref.pointer())
        .isNotEqualTo(refUpdated.pointer());

    //

    soft.assertThat(withCache2.fetchReferenceForUpdate(ref.name())).isEqualTo(refUpdated);
  }

  /** Reference cache TTL expiry. */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void referenceCacheExpiry(boolean bulk) throws Exception {
    // Create ref via instance 1
    Reference ref =
        reference(REF_NAME, randomObjId(), false, withCache1.config().currentTimeMicros(), null);
    withCache1.addReference(ref);

    // Populate cache in instance 2
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(ref);

    // Update ref via instance 1
    Reference refUpdated = withCache1.updateReferencePointer(ref, randomObjId());
    soft.assertThat(refUpdated).isNotEqualTo(ref);

    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isEqualTo(refUpdated);
    // Other test instance did NOT update its cache
    soft.assertThat(fetchRef(withCache2, bulk, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refUpdated.pointer())
        .isEqualTo(ref.pointer())
        .isNotEqualTo(refUpdated.pointer());

    //

    nowNanos.addAndGet(Duration.ofMinutes(2).toNanos());
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(refUpdated);
  }

  /** Tests negative-cache behavior (non-existence of a reference). */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void referenceCacheNegativeExpiry(boolean bulk) throws Exception {
    // Populate both caches w/ negative entries
    soft.assertThat(fetchRef(withCache1, bulk, REF_NAME)).isNull();
    soft.assertThat(fetchRef(withCache2, bulk, REF_NAME)).isNull();

    // Create ref via instance 1
    Reference ref =
        reference(REF_NAME, randomObjId(), false, withCache1.config().currentTimeMicros(), null);
    withCache1.addReference(ref);

    // Cache 1 has "correct" entry
    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isEqualTo(ref);
    // Cache 2 has stale negative entry
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isNull();

    // Expire negative cache entries
    nowNanos.addAndGet(Duration.ofSeconds(2).toNanos());
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(ref);
  }

  static Reference fetchRef(Persist persist, boolean bulk, String refName) {
    return bulk
        ? persist.fetchReferences(new String[] {refName})[0]
        : persist.fetchReference(refName);
  }
}

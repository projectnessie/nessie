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
import org.projectnessie.versioned.storage.common.exceptions.RefAlreadyExistsException;
import org.projectnessie.versioned.storage.common.exceptions.RefConditionFailedException;
import org.projectnessie.versioned.storage.common.exceptions.RefNotFoundException;
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
                .cacheCapacityOvershoot(0.1d)
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

    if (bulk) {
      soft.assertThat(withCache2.fetchReferencesForUpdate(new String[] {ref.name()}))
          .containsExactly(refUpdated);
    } else {
      soft.assertThat(withCache2.fetchReferenceForUpdate(ref.name())).isEqualTo(refUpdated);
    }
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

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void addReference(boolean bulk) throws Exception {
    // Create ref via instance 1
    Reference ref =
        reference(REF_NAME, randomObjId(), false, withCache1.config().currentTimeMicros(), null);
    withCache1.addReference(ref);

    // Try addReference via instance 2
    soft.assertThatThrownBy(() -> withCache2.addReference(ref))
        .isInstanceOf(RefAlreadyExistsException.class);

    // Update ref via instance 1
    Reference refUpdated = withCache1.updateReferencePointer(ref, randomObjId());
    soft.assertThat(refUpdated).isNotEqualTo(ref);

    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isEqualTo(refUpdated);
    // Other test instance DID populate its cache
    soft.assertThat(fetchRef(withCache2, bulk, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refUpdated.pointer())
        .isEqualTo(refUpdated.pointer());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void markReferenceAsDeletedAndPurge(boolean bulk) throws Exception {
    // Create ref via instance 1
    Reference ref =
        reference(REF_NAME, randomObjId(), false, withCache1.config().currentTimeMicros(), null);
    withCache1.addReference(ref);

    // Try addReference via instance 2
    soft.assertThatThrownBy(() -> withCache2.addReference(ref))
        .isInstanceOf(RefAlreadyExistsException.class);

    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(ref);

    // Mark ref as deleted via instance 1
    Reference refDeleted = withCache1.markReferenceAsDeleted(ref);
    soft.assertThat(refDeleted).isNotEqualTo(ref);

    // instance 2 still has the cached deleted instance
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(ref);

    // Try markReferenceAsDeleted via instance 2
    soft.assertThatThrownBy(() -> withCache2.markReferenceAsDeleted(ref))
        .isInstanceOf(RefConditionFailedException.class);

    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isEqualTo(refDeleted);
    // Other test instance DID populate its cache
    soft.assertThat(fetchRef(withCache2, bulk, ref.name()))
        .extracting(Reference::pointer)
        .describedAs("Previous: %s, updated: %s", ref.pointer(), refDeleted.pointer())
        .isEqualTo(refDeleted.pointer());

    //
    // purge
    //

    // Mark ref as deleted via instance 1
    soft.assertThatCode(() -> withCache1.purgeReference(refDeleted)).doesNotThrowAnyException();

    // instance 2 still has the cached deleted instance
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(refDeleted);

    // Try markReferenceAsDeleted via instance 2
    soft.assertThatThrownBy(() -> withCache2.purgeReference(ref))
        .isInstanceOf(RefNotFoundException.class);

    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isNull();
    // Other test instance DID populate its cache
    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void purgeVsUpdate(boolean bulk) throws Exception {
    // Create ref via instance 1
    Reference ref =
        reference(REF_NAME, randomObjId(), false, withCache1.config().currentTimeMicros(), null);
    withCache1.addReference(ref);

    soft.assertThat(fetchRef(withCache2, bulk, ref.name())).isEqualTo(ref);

    // Mark ref as deleted via instance 1
    Reference refDeleted = withCache1.markReferenceAsDeleted(ref);
    soft.assertThat(refDeleted).isNotEqualTo(ref);

    // Mark ref as deleted via instance 1
    soft.assertThatCode(() -> withCache1.purgeReference(refDeleted)).doesNotThrowAnyException();

    soft.assertThat(fetchRef(withCache1, bulk, ref.name())).isNull();

    soft.assertThatThrownBy(() -> withCache2.updateReferencePointer(ref, randomObjId()))
        .isInstanceOf(RefNotFoundException.class);
  }

  static Reference fetchRef(Persist persist, boolean bulk, String refName) {
    return bulk
        ? persist.fetchReferences(new String[] {refName})[0]
        : persist.fetchReference(refName);
  }
}

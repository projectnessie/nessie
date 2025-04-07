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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Reference;
import org.projectnessie.versioned.storage.commontests.objtypes.VersionedTestObj;

@ExtendWith(SoftAssertionsExtension.class)
public class TestDistributedInvalidations {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AtomicLong clockNanos;

  CaffeineCacheBackend backend1noSpy;
  CaffeineCacheBackend backend2noSpy;
  CaffeineCacheBackend backend1;
  CaffeineCacheBackend backend2;

  protected CacheBackend distributed1;
  protected CacheBackend distributed2;

  protected DistributedCacheInvalidation sender1;
  protected DistributedCacheInvalidation sender2;

  @BeforeEach
  public void setup() {
    clockNanos = new AtomicLong();

    backend1noSpy =
        (CaffeineCacheBackend)
            PersistCaches.newBackend(
                CacheConfig.builder()
                    .capacityMb(32)
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofSeconds(1))
                    .clockNanos(clockNanos::get)
                    .cacheCapacityOvershoot(0.1d)
                    .build());
    backend2noSpy =
        (CaffeineCacheBackend)
            PersistCaches.newBackend(
                CacheConfig.builder()
                    .capacityMb(32)
                    .referenceTtl(Duration.ofMinutes(1))
                    .referenceNegativeTtl(Duration.ofSeconds(1))
                    .clockNanos(clockNanos::get)
                    .cacheCapacityOvershoot(0.1d)
                    .build());

    backend1 = spy(backend1noSpy);
    backend2 = spy(backend2noSpy);

    AtomicReference<DistributedCacheInvalidation> emitter1 = new AtomicReference<>();
    AtomicReference<DistributedCacheInvalidation> emitter2 = new AtomicReference<>();

    sender1 = spy(delegate(emitter2::get));
    sender2 = spy(delegate(emitter1::get));

    distributed1 =
        PersistCaches.wrapBackendForDistributedUsage(
            DistributedCacheInvalidations.builder()
                .localBackend(backend1)
                .invalidationSender(sender1)
                .invalidationListenerReceiver(emitter1::set)
                .build());
    distributed2 =
        PersistCaches.wrapBackendForDistributedUsage(
            DistributedCacheInvalidations.builder()
                .localBackend(backend2)
                .invalidationSender(sender2)
                .invalidationListenerReceiver(emitter2::set)
                .build());
  }

  @Test
  public void obj() {
    Obj obj1 =
        VersionedTestObj.builder().id(randomObjId()).versionToken("1").someValue("hello").build();
    Obj obj2 =
        VersionedTestObj.builder().id(obj1.id()).versionToken("2").someValue("again").build();

    distributed1.put("", obj1);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putLocal("", obj1);
    verify(backend2).remove("", obj1.id());
    verify(sender1).evictObj("", obj1.id());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    soft.assertThat(backend1noSpy.get("", obj1.id())).isEqualTo(obj1);
    soft.assertThat(backend2noSpy.get("", obj1.id())).isNull();

    // Simulate that backend2 loaded obj1 in the meantime
    backend2noSpy.put("", obj1);
    soft.assertThat(backend2noSpy.get("", obj1.id())).isEqualTo(obj1);

    distributed1.put("", obj2);
    soft.assertThat(backend2noSpy.get("", obj1.id())).isNull();

    verify(backend1).cachePut(any(), any());
    verify(backend1).putLocal("", obj2);
    verify(backend2).remove("", obj1.id());
    verify(sender1).evictObj("", obj2.id());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Simulate that backend2 loaded obj2 in the meantime
    backend2noSpy.put("", obj2);
    soft.assertThat(backend2noSpy.get("", obj2.id())).isEqualTo(obj2);

    // update to same object (still a removal for backend2)

    distributed1.put("", obj2);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putLocal("", obj2);
    verify(backend2).remove("", obj2.id());
    verify(sender1).evictObj("", obj2.id());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Verify that ref2 has not been removed (same hash)
    soft.assertThat(backend2noSpy.get("", obj2.id())).isNull();

    // remove object

    distributed1.remove("", obj2.id());

    verify(backend1).remove("", obj2.id());
    verify(backend2).remove("", obj2.id());
    verify(sender1).evictObj("", obj2.id());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();
  }

  @Test
  public void reference() {
    Reference ref1 = Reference.reference("refs/foo/bar", randomObjId(), false, 0L, null);
    Reference ref2 = ref1.forNewPointer(randomObjId(), StoreConfig.Adjustable.empty());

    distributed1.putReference("", ref1);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putReferenceLocal("", ref1);
    verify(backend2).removeReference("", ref1.name());
    verify(sender1).evictReference("", ref1.name());
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    soft.assertThat(backend1noSpy.getReference("", ref1.name())).isEqualTo(ref1);
    soft.assertThat(backend2noSpy.getReference("", ref1.name())).isNull();

    // Simulate that backend2 loaded ref1 in the meantime
    backend2noSpy.putReference("", ref1);
    soft.assertThat(backend2noSpy.getReference("", ref1.name())).isEqualTo(ref1);

    distributed1.putReference("", ref2);
    soft.assertThat(backend2noSpy.getReference("", ref1.name())).isNull();

    verify(backend1).cachePut(any(), any());
    verify(backend1).putReferenceLocal("", ref2);
    verify(backend2).removeReference("", ref1.name());
    verify(backend2).removeReference("", ref1.name());
    verify(sender1).evictReference("", ref2.name());
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Simulate that backend2 loaded ref2 in the meantime
    backend2noSpy.putReference("", ref2);
    soft.assertThat(backend2noSpy.getReference("", ref2.name())).isEqualTo(ref2);

    // update to same reference (no change for backend2)

    distributed1.putReference("", ref2);

    verify(backend1).cachePut(any(), any());
    verify(backend1).putReferenceLocal("", ref2);
    verify(backend2).removeReference("", ref2.name());
    verify(sender1).evictReference("", ref2.name());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();

    // Verify that ref2 has been removed in backend2
    soft.assertThat(backend2noSpy.getReference("", ref2.name())).isNull();

    // remove reference

    distributed1.removeReference("", ref2.name());

    verify(backend1).removeReference("", ref2.name());
    verify(backend2).removeReference("", ref2.name());
    verify(sender1).evictReference("", ref2.name());
    verifyNoMoreInteractions(backend1);
    verifyNoMoreInteractions(backend2);
    verifyNoMoreInteractions(sender1);
    verifyNoMoreInteractions(sender2);
    resetAll();
  }

  private void resetAll() {
    reset(backend1);
    reset(backend2);
    reset(sender1);
    reset(sender2);
  }

  protected static DistributedCacheInvalidation delegate(
      Supplier<DistributedCacheInvalidation> invalidation) {
    return new DistributedCacheInvalidation() {
      @Override
      public void evictObj(String repositoryId, ObjId objId) {
        invalidation.get().evictObj(repositoryId, objId);
      }

      @Override
      public void evictReference(String repositoryId, String refName) {
        invalidation.get().evictReference(repositoryId, refName);
      }
    };
  }
}

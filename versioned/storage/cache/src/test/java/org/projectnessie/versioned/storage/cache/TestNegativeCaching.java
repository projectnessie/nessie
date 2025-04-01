/*
 * Copyright (C) 2022 Dremio
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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.cache.CacheTestObjTypeBundle.NegativeCachingObj;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestNegativeCaching {

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void nonEffectiveNegativeCache(boolean enableSoftReferences) throws Exception {
    Persist backing = spy(persist);
    CacheBackend cacheBackend =
        PersistCaches.newBackend(
            CacheConfig.builder()
                .capacityMb(16)
                .enableSoftReferences(enableSoftReferences)
                .cacheCapacityOvershoot(0.1d)
                .build());
    Persist cachedPersist = spy(cacheBackend.wrap(backing));

    verify(backing).config();
    reset(backing);

    ObjId id = randomObjId();

    soft.assertThatThrownBy(() -> cachedPersist.fetchObj(id))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist).fetchObj(id);
    verify(backing).fetchObj(id);
    verify(backing).fetchTypedObj(eq(id), any(), any());
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    // repeat
    soft.assertThatThrownBy(() -> cachedPersist.fetchObj(id))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist).fetchObj(id);
    verify(backing).fetchObj(id);
    verify(backing).fetchTypedObj(eq(id), any(), any());
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void negativeCacheFetchTypedObj(boolean enableSoftReferences) throws Exception {
    Persist backing = spy(persist);
    CacheBackend cacheBackend =
        PersistCaches.newBackend(
            CacheConfig.builder()
                .capacityMb(16)
                .enableSoftReferences(enableSoftReferences)
                .cacheCapacityOvershoot(0.1d)
                .build());
    Persist cachedPersist = spy(cacheBackend.wrap(backing));

    verify(backing).config();
    reset(backing);

    ObjId id = randomObjId();

    soft.assertThatThrownBy(
            () ->
                cachedPersist.fetchTypedObj(id, NegativeCachingObj.TYPE, NegativeCachingObj.class))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist).fetchTypedObj(id, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    verify(backing).fetchTypedObj(id, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    negativeCacheFetchRepeat(cachedPersist, backing, id);
  }

  private void negativeCacheFetchRepeat(Persist cachedPersist, Persist backing, ObjId id)
      throws Exception {
    soft.assertThatThrownBy(
            () ->
                cachedPersist.fetchTypedObj(id, NegativeCachingObj.TYPE, NegativeCachingObj.class))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist).fetchTypedObj(id, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    soft.assertThatThrownBy(() -> cachedPersist.fetchObj(id))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist).fetchObj(id);
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    soft.assertThat(cachedPersist.getImmediate(id)).isNull();
    verify(cachedPersist).getImmediate(id);
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    soft.assertThatThrownBy(() -> cachedPersist.fetchObjType(id))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist).fetchObjType(id);
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    soft.assertThatThrownBy(
            () ->
                cachedPersist.fetchTypedObjs(
                    new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist)
        .fetchTypedObjs(new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    soft.assertThat(
            cachedPersist.fetchTypedObjsIfExist(
                new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class))
        .hasSize(1)
        .containsOnlyNulls();
    verify(cachedPersist)
        .fetchTypedObjsIfExist(new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    soft.assertThat(cachedPersist.fetchObjsIfExist(new ObjId[] {id}))
        .hasSize(1)
        .containsOnlyNulls();
    verify(cachedPersist).fetchObjsIfExist(new ObjId[] {id});
    // backing.fetch*() must not be invoked
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void negativeCacheFetchTypedObjs(boolean enableSoftReferences) throws Exception {
    Persist backing = spy(persist);
    CacheBackend cacheBackend =
        PersistCaches.newBackend(
            CacheConfig.builder()
                .capacityMb(16)
                .enableSoftReferences(enableSoftReferences)
                .cacheCapacityOvershoot(0.1d)
                .build());
    Persist cachedPersist = spy(cacheBackend.wrap(backing));

    verify(backing).config();
    reset(backing);

    ObjId id = randomObjId();

    soft.assertThatThrownBy(
            () ->
                cachedPersist.fetchTypedObjs(
                    new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class))
        .isInstanceOf(ObjNotFoundException.class);
    verify(cachedPersist)
        .fetchTypedObjs(new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    verify(backing)
        .fetchTypedObjsIfExist(new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    negativeCacheFetchRepeat(cachedPersist, backing, id);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void negativeCacheFetchTypedObjsIfExist(boolean enableSoftReferences) throws Exception {
    Persist backing = spy(persist);
    CacheBackend cacheBackend =
        PersistCaches.newBackend(
            CacheConfig.builder()
                .capacityMb(16)
                .enableSoftReferences(enableSoftReferences)
                .cacheCapacityOvershoot(0.1d)
                .build());
    Persist cachedPersist = spy(cacheBackend.wrap(backing));

    verify(backing).config();
    reset(backing);

    ObjId id = randomObjId();

    soft.assertThat(
            cachedPersist.fetchTypedObjsIfExist(
                new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class))
        .hasSize(1)
        .containsOnlyNulls();
    verify(cachedPersist)
        .fetchTypedObjsIfExist(new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    verify(backing)
        .fetchTypedObjsIfExist(new ObjId[] {id}, NegativeCachingObj.TYPE, NegativeCachingObj.class);
    verifyNoMoreInteractions(backing, cachedPersist);
    reset(backing, cachedPersist);

    negativeCacheFetchRepeat(cachedPersist, backing, id);
  }
}

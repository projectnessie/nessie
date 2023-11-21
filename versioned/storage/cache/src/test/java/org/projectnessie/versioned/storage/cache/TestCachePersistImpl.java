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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig.Adjustable;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.ContentValueObj;
import org.projectnessie.versioned.storage.common.objtypes.StringObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestCachePersistImpl {

  static final ContentValueObj OBJ1 =
      ContentValueObj.contentValue("1", 1, ByteString.copyFromUtf8("test1"));
  static final ContentValueObj OBJ2 =
      ContentValueObj.contentValue("2", 2, ByteString.copyFromUtf8("test2"));
  static final ContentValueObj OBJ3 =
      ContentValueObj.contentValue("3", 3, ByteString.copyFromUtf8("test3"));

  @InjectSoftAssertions SoftAssertions soft;

  private Persist persist;
  private ObjCache cache;
  private CachingPersistImpl cachingPersist;

  @BeforeEach
  void setUp(@NessiePersist PersistFactory persistFactory) {
    Adjustable storeConfig = Adjustable.empty().withRepositoryId("test");
    Persist p = persistFactory.newPersist(storeConfig);
    p.erase();
    persist = Mockito.spy(p);
    cache = new ObjCacheImpl(PersistCaches.newBackend(1, null), "test");
    cachingPersist = new CachingPersistImpl(persist, cache);
  }

  @Test
  void fetchObjCacheHit() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    cache.put(OBJ1);
    soft.assertThat(cachingPersist.fetchObj(OBJ1.id())).isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist, never()).fetchObj(OBJ1.id());
  }

  @Test
  void fetchObjCacheMiss() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    soft.assertThat(cachingPersist.fetchObj(OBJ1.id())).isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist).fetchObj(OBJ1.id());
  }

  @Test
  void fetchObjNotFound() throws ObjNotFoundException {
    soft.assertThatExceptionOfType(ObjNotFoundException.class)
        .isThrownBy(() -> cachingPersist.fetchObj(OBJ1.id()));
    soft.assertThat(cache.get(OBJ1.id())).isNull();
    verify(persist).fetchObj(OBJ1.id());
  }

  @Test
  void fetchTypedObjCacheHit() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    cache.put(OBJ1);
    soft.assertThat(cachingPersist.fetchTypedObj(OBJ1.id(), OBJ1.type(), ContentValueObj.class))
        .isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist, never()).fetchTypedObj(OBJ1.id(), OBJ1.type(), ContentValueObj.class);
  }

  @Test
  void fetchTypedObjCacheHitWrongType() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    cache.put(OBJ1);
    soft.assertThatExceptionOfType(ObjNotFoundException.class)
        .isThrownBy(() -> cachingPersist.fetchTypedObj(OBJ1.id(), ObjType.STRING, StringObj.class));
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist, never()).fetchTypedObj(OBJ1.id(), ObjType.STRING, StringObj.class);
  }

  @Test
  void fetchTypedObjCacheMiss() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    soft.assertThat(cachingPersist.fetchTypedObj(OBJ1.id(), OBJ1.type(), ContentValueObj.class))
        .isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist).fetchTypedObj(OBJ1.id(), OBJ1.type(), ContentValueObj.class);
  }

  @Test
  void fetchTypedObjNotFound() throws ObjNotFoundException {
    soft.assertThatExceptionOfType(ObjNotFoundException.class)
        .isThrownBy(
            () -> cachingPersist.fetchTypedObj(OBJ1.id(), OBJ1.type(), ContentValueObj.class));
    soft.assertThat(cache.get(OBJ1.id())).isNull();
    verify(persist).fetchTypedObj(OBJ1.id(), OBJ1.type(), ContentValueObj.class);
  }

  @Test
  void fetchObjTypeCacheHit() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    cache.put(OBJ1);
    soft.assertThat(cachingPersist.fetchObjType(OBJ1.id())).isEqualTo(OBJ1.type());
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist, never()).fetchObjType(OBJ1.id());
  }

  @Test
  void fetchObjTypeCacheMiss() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObj(OBJ1);
    soft.assertThat(cachingPersist.fetchObjType(OBJ1.id())).isEqualTo(OBJ1.type());
    soft.assertThat(cache.get(OBJ1.id())).isNull(); // does NOT update the cache
    verify(persist).fetchObjType(OBJ1.id());
  }

  @Test
  void fetchObjsFullCacheHit() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObjs(new Obj[] {OBJ1, OBJ2, OBJ3});
    cache.put(OBJ1);
    cache.put(OBJ2);
    cache.put(OBJ3);
    soft.assertThat(cachingPersist.fetchObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()}))
        .containsExactly(null, OBJ1, OBJ2, OBJ3);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ2.id())).isEqualTo(OBJ2);
    soft.assertThat(cache.get(OBJ3.id())).isEqualTo(OBJ3);
    verify(persist, never()).fetchObjs(any());
  }

  @Test
  void fetchObjsPartialCacheHit() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObjs(new Obj[] {OBJ1, OBJ2, OBJ3});
    cache.put(OBJ1);
    soft.assertThat(cachingPersist.fetchObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()}))
        .containsExactly(null, OBJ1, OBJ2, OBJ3);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ2.id())).isEqualTo(OBJ2);
    soft.assertThat(cache.get(OBJ3.id())).isEqualTo(OBJ3);
    verify(persist).fetchObjs(new ObjId[] {null, null, OBJ2.id(), OBJ3.id()});
  }

  @Test
  void fetchObjsFullCacheMiss() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObjs(new Obj[] {OBJ1, OBJ2, OBJ3});
    soft.assertThat(cachingPersist.fetchObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()}))
        .containsExactly(null, OBJ1, OBJ2, OBJ3);
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ2.id())).isEqualTo(OBJ2);
    soft.assertThat(cache.get(OBJ3.id())).isEqualTo(OBJ3);
    verify(persist).fetchObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()});
  }

  @Test
  void fetchObjsNotFound() throws ObjNotFoundException, ObjTooLargeException {
    persist.storeObjs(new Obj[] {OBJ1, OBJ2});
    cache.put(OBJ1);
    soft.assertThatThrownBy(
            () -> cachingPersist.fetchObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()}))
        .isInstanceOf(ObjNotFoundException.class)
        .hasMessageContaining(OBJ3.id().toString());
    soft.assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    soft.assertThat(cache.get(OBJ2.id())).isNull(); // cache not updated
    soft.assertThat(cache.get(OBJ3.id())).isNull();
    verify(persist).fetchObjs(new ObjId[] {null, null, OBJ2.id(), OBJ3.id()});
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void storeObjSuccess(boolean ignoreSoftSizeRestrictions) throws ObjTooLargeException {
    assertThat(cachingPersist.storeObj(OBJ1, ignoreSoftSizeRestrictions)).isTrue();
    assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist).storeObj(OBJ1, ignoreSoftSizeRestrictions);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void storeObjFailure(boolean ignoreSoftSizeRestrictions) throws ObjTooLargeException {
    persist.upsertObj(OBJ1);
    assertThat(cachingPersist.storeObj(OBJ1, ignoreSoftSizeRestrictions)).isFalse();
    assertThat(cache.get(OBJ1.id())).isNull();
    verify(persist).storeObj(OBJ1, ignoreSoftSizeRestrictions);
  }

  @Test
  void storeObjTooLarge() throws ObjTooLargeException {
    doThrow(new ObjTooLargeException(0, 0)).when(persist).storeObj(OBJ1, false);
    assertThatExceptionOfType(ObjTooLargeException.class)
        .isThrownBy(() -> cachingPersist.storeObj(OBJ1, false));
    assertThat(cache.get(OBJ1.id())).isNull();
    verify(persist).storeObj(OBJ1, false);
  }

  @Test
  void storeObjsAllSuccessful() throws ObjTooLargeException {
    assertThat(cachingPersist.storeObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3}))
        .containsExactly(false, true, true, true);
    assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    assertThat(cache.get(OBJ2.id())).isEqualTo(OBJ2);
    assertThat(cache.get(OBJ3.id())).isEqualTo(OBJ3);
    verify(persist).storeObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3});
  }

  @Test
  void storeObjsPartiallySuccessful() throws ObjTooLargeException {
    persist.upsertObj(OBJ1);
    assertThat(cachingPersist.storeObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3}))
        .containsExactly(false, false, true, true);
    assertThat(cache.get(OBJ1.id())).isNull();
    assertThat(cache.get(OBJ2.id())).isEqualTo(OBJ2);
    assertThat(cache.get(OBJ3.id())).isEqualTo(OBJ3);
    verify(persist).storeObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3});
  }

  @Test
  void storeObjsNoneSuccessful() throws ObjTooLargeException {
    persist.upsertObjs(new Obj[] {OBJ1, OBJ2, OBJ3});
    assertThat(cachingPersist.storeObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3}))
        .containsExactly(false, false, false, false);
    assertThat(cache.get(OBJ1.id())).isNull();
    assertThat(cache.get(OBJ2.id())).isNull();
    assertThat(cache.get(OBJ3.id())).isNull();
    verify(persist).storeObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3});
  }

  @Test
  void upsertObj() throws ObjTooLargeException {
    cachingPersist.upsertObj(OBJ1);
    assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    verify(persist).upsertObj(OBJ1);
  }

  @Test
  void upsertObjTooLarge() throws ObjTooLargeException {
    doThrow(new ObjTooLargeException(0, 0)).when(persist).upsertObj(OBJ1);
    assertThatExceptionOfType(ObjTooLargeException.class)
        .isThrownBy(() -> cachingPersist.upsertObj(OBJ1));
    assertThat(cache.get(OBJ1.id())).isNull();
    verify(persist).upsertObj(OBJ1);
  }

  @Test
  void upsertObjs() throws ObjTooLargeException {
    cachingPersist.upsertObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3});
    assertThat(cache.get(OBJ1.id())).isEqualTo(OBJ1);
    assertThat(cache.get(OBJ2.id())).isEqualTo(OBJ2);
    assertThat(cache.get(OBJ3.id())).isEqualTo(OBJ3);
    verify(persist).upsertObjs(new Obj[] {null, OBJ1, OBJ2, OBJ3});
  }

  @Test
  void deleteObj() throws ObjTooLargeException {
    persist.storeObj(OBJ1);
    cache.put(OBJ1);
    cachingPersist.deleteObj(OBJ1.id());
    assertThat(cache.get(OBJ1.id())).isNull();
    verify(persist).deleteObj(OBJ1.id());
  }

  @Test
  void deleteObjs() throws ObjTooLargeException {
    persist.storeObjs(new Obj[] {OBJ1, OBJ2, OBJ3});
    cache.put(OBJ1);
    cache.put(OBJ2);
    cache.put(OBJ3);
    cachingPersist.deleteObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()});
    assertThat(cache.get(OBJ1.id())).isNull();
    assertThat(cache.get(OBJ2.id())).isNull();
    assertThat(cache.get(OBJ3.id())).isNull();
    verify(persist).deleteObjs(new ObjId[] {null, OBJ1.id(), OBJ2.id(), OBJ3.id()});
  }

  @Test
  void erase() {
    cache.put(OBJ1);
    cache.put(OBJ2);
    cache.put(OBJ3);
    cachingPersist.erase();
    assertThat(cache.get(OBJ1.id())).isNull();
    assertThat(cache.get(OBJ2.id())).isNull();
    assertThat(cache.get(OBJ3.id())).isNull();
    verify(persist).erase();
  }
}

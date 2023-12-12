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
package org.projectnessie.versioned.storage.batching;

import static java.util.Arrays.stream;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.commontests.AbstractBasePersistTests.updateObjChange;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.exceptions.ObjTooLargeException;
import org.projectnessie.versioned.storage.common.objtypes.CommitHeaders;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.commontests.AbstractBasePersistTests;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackend;
import org.projectnessie.versioned.storage.inmemory.InmemoryBackendFactory;

@ExtendWith(SoftAssertionsExtension.class)
public class TestBatchingPersist {
  @InjectSoftAssertions protected SoftAssertions soft;
  private Persist base;
  private BatchingPersistImpl batching;

  @BeforeEach
  void setup() {
    base = base();
    batching = batching(base);
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  void singleObj(Obj obj) throws Exception {
    // Obj that exists in "base"
    base.storeObj(obj);
    soft.assertThat(batching.fetchObj(obj.id())).isEqualTo(obj);
    base.deleteObj(obj.id());
    soft.assertThatThrownBy(() -> batching.fetchObj(obj.id()))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> batching.fetchObjs(new ObjId[] {obj.id()}))
        .isInstanceOf(ObjNotFoundException.class);

    // Obj stored via "batching"
    batching.storeObj(obj);
    soft.assertThat(batching.pendingStores()).containsExactly(entry(obj.id(), obj));
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    soft.assertThatThrownBy(() -> base.fetchObj(obj.id())).isInstanceOf(ObjNotFoundException.class);
    soft.assertThatThrownBy(() -> base.fetchObjs(new ObjId[] {obj.id()}))
        .isInstanceOf(ObjNotFoundException.class);
    soft.assertThat(batching.fetchObj(obj.id())).isEqualTo(obj);
    soft.assertThat(batching.fetchObjs(new ObjId[] {obj.id()})).containsExactly(obj);
    batching.flush();
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    soft.assertThat(base.fetchObj(obj.id())).isEqualTo(obj);
    soft.assertThat(base.fetchObjs(new ObjId[] {obj.id()})).containsExactly(obj);
    soft.assertThat(batching.fetchObj(obj.id())).isEqualTo(obj);
    soft.assertThat(batching.fetchObjs(new ObjId[] {obj.id()})).containsExactly(obj);

    // Obj updated via "batching"
    Obj updated = updateObjChange(obj);
    batching.upsertObj(updated);
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).containsExactly(entry(obj.id(), updated));
    soft.assertThat(base.fetchObj(obj.id())).isEqualTo(obj);
    soft.assertThat(base.fetchObjs(new ObjId[] {obj.id()})).containsExactly(obj);
    soft.assertThat(batching.fetchObj(obj.id())).isEqualTo(updated);
    soft.assertThat(batching.fetchObjs(new ObjId[] {obj.id()})).containsExactly(updated);
    batching.flush();
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    soft.assertThat(base.fetchObj(obj.id())).isEqualTo(updated);
    soft.assertThat(base.fetchObjs(new ObjId[] {obj.id()})).containsExactly(updated);
    soft.assertThat(batching.fetchObj(obj.id())).isEqualTo(updated);
    soft.assertThat(batching.fetchObjs(new ObjId[] {obj.id()})).containsExactly(updated);
  }

  @Test
  void multipleObjs() throws Exception {
    Obj[] objs = allObjectTypeSamples().toArray(Obj[]::new);
    ObjId[] ids = stream(objs).map(Obj::id).toArray(ObjId[]::new);
    Map<ObjId, Obj> objsMap = stream(objs).collect(Collectors.toMap(Obj::id, o -> o));

    // Obj that exists in "base"
    base.storeObjs(objs);
    soft.assertThat(batching.fetchObjs(ids)).containsExactly(objs);
    base.deleteObjs(ids);
    soft.assertThatThrownBy(() -> batching.fetchObjs(ids))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds)
        .asInstanceOf(list(ObjId.class))
        .containsExactly(ids);

    // Obj stored via "batching"
    batching.storeObjs(objs);
    soft.assertThat(batching.pendingStores()).isEqualTo(objsMap);
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    soft.assertThatThrownBy(() -> base.fetchObjs(ids))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds)
        .asInstanceOf(list(ObjId.class))
        .containsExactly(ids);
    soft.assertThat(batching.fetchObjs(ids)).containsExactly(objs);
    batching.flush();
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    soft.assertThat(base.fetchObjs(ids)).containsExactly(objs);
    soft.assertThat(batching.fetchObjs(ids)).containsExactly(objs);

    // Obj updated via "batching"
    Obj[] updated = stream(objs).map(AbstractBasePersistTests::updateObjChange).toArray(Obj[]::new);
    Map<ObjId, Obj> updatedMap = stream(updated).collect(Collectors.toMap(Obj::id, o -> o));
    batching.upsertObjs(updated);
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEqualTo(updatedMap);
    soft.assertThat(base.fetchObjs(ids)).containsExactly(objs);
    soft.assertThat(batching.fetchObjs(ids)).containsExactly(updated);
    batching.flush();
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    soft.assertThat(base.fetchObjs(ids)).containsExactly(updated);
    soft.assertThat(batching.fetchObjs(ids)).containsExactly(updated);
  }

  @Test
  void erase() throws Exception {
    Obj[] objs = allObjectTypeSamples().toArray(Obj[]::new);

    batching.storeObjs(objs);
    soft.assertThat(batching.pendingStores()).hasSize(objs.length);
    soft.assertThat(batching.pendingUpserts()).isEmpty();
    batching.erase();
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEmpty();

    Obj[] updated = stream(objs).map(AbstractBasePersistTests::updateObjChange).toArray(Obj[]::new);
    batching.upsertObjs(updated);
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).hasSize(objs.length);
    batching.erase();
    soft.assertThat(batching.pendingStores()).isEmpty();
    soft.assertThat(batching.pendingUpserts()).isEmpty();
  }

  @Test
  void tooLarge() {
    CommitObj o =
        CommitObj.commitBuilder()
            .id(randomObjId())
            .created(0L)
            .seq(0L)
            .headers(CommitHeaders.EMPTY_COMMIT_HEADERS)
            .message("")
            .incrementalIndex(
                ByteString.copyFrom(new byte[base.effectiveIncrementalIndexSizeLimit() + 1]))
            .build();
    soft.assertThatThrownBy(() -> batching.storeObj(o)).isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(() -> batching.storeObjs(new Obj[] {o}))
        .isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(() -> batching.upsertObj(o)).isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(() -> batching.upsertObjs(new Obj[] {o}))
        .isInstanceOf(ObjTooLargeException.class);

    IndexObj s =
        IndexObj.index(ByteString.copyFrom(new byte[base.effectiveIndexSegmentSizeLimit() + 1]));
    soft.assertThatThrownBy(() -> batching.storeObj(s)).isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(() -> batching.storeObjs(new Obj[] {s}))
        .isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(() -> batching.upsertObj(s)).isInstanceOf(ObjTooLargeException.class);
    soft.assertThatThrownBy(() -> batching.upsertObjs(new Obj[] {s}))
        .isInstanceOf(ObjTooLargeException.class);
  }

  @ParameterizedTest
  @MethodSource("allObjectTypeSamples")
  void noFlush(Obj obj) throws Exception {
    BatchingPersistImpl persist =
        (BatchingPersistImpl) WriteBatching.builder().persist(base).batchSize(0).build().create();
    persist.storeObj(obj);
    Obj updated = updateObjChange(obj);
    persist.upsertObj(updated);
    persist.flush();
    soft.assertThat(persist.pendingStores()).containsExactly(entry(obj.id(), obj));
    soft.assertThat(persist.pendingUpserts()).containsExactly(entry(obj.id(), updated));
  }

  private Persist base() {
    InmemoryBackendFactory factory = new InmemoryBackendFactory();
    @SuppressWarnings("resource")
    InmemoryBackend backend = factory.buildBackend(factory.newConfigInstance());
    return backend.createFactory().newPersist(StoreConfig.Adjustable.empty());
  }

  private BatchingPersistImpl batching(Persist base) {
    return (BatchingPersistImpl)
        WriteBatching.builder()
            .persist(base)
            .batchSize((int) allObjectTypeSamples().count())
            .build()
            .create();
  }

  static Stream<Obj> allObjectTypeSamples() {
    return AbstractBasePersistTests.allObjectTypeSamples();
  }

  // plain delegates are untested (delegation code generated by IntelliJ)
}

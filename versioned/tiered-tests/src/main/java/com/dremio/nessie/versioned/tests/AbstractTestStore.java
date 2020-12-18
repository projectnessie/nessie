/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.nessie.versioned.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.LoadOp;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * Common class for testing public APIs of a Store.
 * This class should be moved to the versioned/tests project when it will not introduce a circular dependency.
 * @param <S> The type of the Store being tested.
 */
public abstract class AbstractTestStore<S extends Store> {
  private static class CreatorPair {
    final ValueType type;
    final Supplier<HasId> supplier;

    CreatorPair(ValueType type, Supplier<HasId> supplier) {
      this.type = type;
      this.supplier = supplier;
    }
  }

  protected Random random;
  protected S store;

  /**
   * Create and start the store, if not already done.
   */
  @BeforeEach
  void setup() {
    if (store == null) {
      this.store = createStore();
      this.store.start();
      random = new Random(getRandomSeed());
    }
  }

  /**
   * Reset the state of the store.
   */
  @AfterEach
  void reset() {
    resetStoreState();
  }

  /**
   * Create the store that will be tested.
   * @return the newly created (unstarted) store.
   */
  protected abstract S createStore();

  protected abstract long getRandomSeed();

  protected abstract void resetStoreState();

  @Test
  void load() {
    final ImmutableList<CreatorPair> creators = ImmutableList.<CreatorPair>builder()
        .add(new CreatorPair(ValueType.REF, () -> SampleEntities.createTag(random)))
        .add(new CreatorPair(ValueType.REF, () -> SampleEntities.createBranch(random)))
        .add(new CreatorPair(ValueType.COMMIT_METADATA, () -> SampleEntities.createCommitMetadata(random)))
        .add(new CreatorPair(ValueType.VALUE, () -> SampleEntities.createValue(random)))
        .add(new CreatorPair(ValueType.L1, () -> SampleEntities.createL1(random)))
        .add(new CreatorPair(ValueType.L2, () -> SampleEntities.createL2(random)))
        .add(new CreatorPair(ValueType.L3, () -> SampleEntities.createL3(random)))
        .add(new CreatorPair(ValueType.KEY_FRAGMENT, () -> SampleEntities.createFragment(random)))
        .build();

    final ImmutableMultimap.Builder<ValueType, HasId> builder = ImmutableMultimap.builder();
    for (int i = 0; i < 100; ++i) {
      final int index = i % creators.size();
      final HasId obj = creators.get(index).supplier.get();
      builder.put(creators.get(index).type, obj);
    }

    final Multimap<ValueType, HasId> objs = builder.build();
    objs.forEach(this::putThenLoad);

    testLoad(objs);
  }

  @Test
  void loadSteps() {
    final Multimap<ValueType, HasId> objs = ImmutableMultimap.<ValueType, HasId>builder()
        .put(ValueType.REF, SampleEntities.createBranch(random))
        .put(ValueType.REF, SampleEntities.createBranch(random))
        .put(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random))
        .build();

    final Multimap<ValueType, HasId> objs2 = ImmutableMultimap.<ValueType, HasId>builder()
        .put(ValueType.L3, SampleEntities.createL3(random))
        .put(ValueType.VALUE, SampleEntities.createValue(random))
        .put(ValueType.VALUE, SampleEntities.createValue(random))
        .put(ValueType.VALUE, SampleEntities.createValue(random))
        .put(ValueType.REF, SampleEntities.createTag(random))
        .build();

    objs.forEach(this::putThenLoad);
    objs2.forEach(this::putThenLoad);

    final LoadStep step2 = createTestLoadStep(objs2);
    final LoadStep step1 = createTestLoadStep(objs, Optional.of(step2));

    store.load(step1);
  }

  @Test
  void loadNone() {
    testLoad(ImmutableMultimap.of());
  }

  @Test
  void loadInvalid() {
    putThenLoad(ValueType.REF, SampleEntities.createBranch(random));
    final Multimap<ValueType, HasId> objs = ImmutableMultimap.of(ValueType.REF, SampleEntities.createBranch(random));

    Assertions.assertThrows(NotFoundException.class, () -> testLoad(objs));
  }

  @Test
  void loadSingleInvalid() {
    Assertions.assertThrows(NotFoundException.class, () -> store.loadSingle(ValueType.REF, SampleEntities.createId(random)));
  }

  @Test
  void loadSingleL1() {
    putThenLoad(ValueType.L1, SampleEntities.createL1(random));
  }

  @Test
  void loadSingleL2() {
    putThenLoad(ValueType.L2, SampleEntities.createL2(random));
  }

  @Test
  void loadSingleL3() {
    putThenLoad(ValueType.L3, SampleEntities.createL3(random));
  }

  @Test
  void loadFragment() {
    putThenLoad(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random));
  }

  @Test
  void loadBranch() {
    putThenLoad(ValueType.REF, SampleEntities.createBranch(random));
  }

  @Test
  void loadTag() {
    putThenLoad(ValueType.REF, SampleEntities.createTag(random));
  }

  @Test
  void loadCommitMetadata() {
    putThenLoad(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
  }

  @Test
  void loadValue() {
    putThenLoad(ValueType.VALUE, SampleEntities.createValue(random));
  }

  @Test
  void putIfAbsentL1() {
    testPutIfAbsent(ValueType.L1, SampleEntities.createL1(random));
  }

  @Test
  void putIfAbsentL2() {
    testPutIfAbsent(ValueType.L2, SampleEntities.createL2(random));
  }

  @Test
  void putIfAbsentL3() {
    testPutIfAbsent(ValueType.L3, SampleEntities.createL3(random));
  }

  @Test
  void putIfAbsentFragment() {
    testPutIfAbsent(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random));
  }

  @Test
  void putIfAbsentBranch() {
    testPutIfAbsent(ValueType.REF, SampleEntities.createBranch(random));
  }

  @Test
  void putIfAbsentTag() {
    testPutIfAbsent(ValueType.REF, SampleEntities.createTag(random));
  }

  @Test
  void putIfAbsentCommitMetadata() {
    testPutIfAbsent(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
  }

  @Test
  void putIfAbsentValue() {
    testPutIfAbsent(ValueType.VALUE, SampleEntities.createValue(random));
  }

  @Test
  void save() {
    final L1 l1 = SampleEntities.createL1(random);
    final InternalRef branch = SampleEntities.createBranch(random);
    final InternalRef tag = SampleEntities.createTag(random);
    final List<SaveOp<?>> saveOps = ImmutableList.of(
        new SaveOp<>(ValueType.L1, l1),
        new SaveOp<>(ValueType.REF, branch),
        new SaveOp<>(ValueType.REF, tag)
    );
    store.save(saveOps);

    saveOps.forEach(s -> {
      try {
        final SimpleSchema<Object> schema = s.getType().getSchema();
        assertEquals(
            schema.itemToMap(s.getValue(), true),
            schema.itemToMap(store.loadSingle(s.getType(), s.getValue().getId()), true));
      } catch (NotFoundException e) {
        Assertions.fail(e);
      }
    });
  }

  protected <T extends HasId> void putThenLoad(ValueType type, T sample) {
    store.put(type, sample, Optional.empty());
    testLoadSingle(type, sample);
  }

  protected void testLoad(Multimap<ValueType, HasId> objs) {
    store.load(createTestLoadStep(objs));
  }

  LoadStep createTestLoadStep(Multimap<ValueType, HasId> objs) {
    return createTestLoadStep(objs, Optional.empty());
  }

  LoadStep createTestLoadStep(Multimap<ValueType, HasId> objs, Optional<LoadStep> next) {
    return new LoadStep(
        objs.entries().stream().map(e -> new LoadOp<>(e.getKey(), e.getValue().getId(),
            r -> assertEquals(e.getKey().getSchema().itemToMap(e.getValue(), true),
                e.getKey().getSchema().itemToMap(r, true)))
        ).collect(Collectors.toList()),
        () -> next
    );
  }

  protected <T extends HasId> void testLoadSingle(ValueType type, T sample) {
    final T read = store.loadSingle(type, sample.getId());
    final SimpleSchema<T> schema = type.getSchema();
    assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
  }

  protected <T extends HasId> void testPutIfAbsent(ValueType type, T sample) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    testLoadSingle(type, sample);
    Assertions.assertFalse(store.putIfAbsent(type, sample));
    testLoadSingle(type, sample);
  }
}

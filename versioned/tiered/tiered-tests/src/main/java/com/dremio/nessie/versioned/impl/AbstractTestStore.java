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
package com.dremio.nessie.versioned.impl;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.tiered.builder.BaseValue;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.ConditionFailedException;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.LoadStep;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
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
  protected static final ExpressionPath COMMITS = ExpressionPath.builder("commits").build();
  protected static final Entity ONE = Entity.ofNumber(1);
  protected static final Entity TWO = Entity.ofNumber(2);

  private static class CreatorPair {
    final ValueType<?> type;
    final Supplier<HasId> supplier;

    CreatorPair(ValueType<?> type, Supplier<HasId> supplier) {
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

  protected S createRawStore() {
    return createStore();
  }

  protected abstract long getRandomSeed();

  protected abstract void resetStoreState();

  protected abstract int loadSize();

  @Test
  void closeWithoutStart() {
    final Store localStore = createRawStore();
    localStore.close(); // This should be a no-op.
  }

  @Test
  void closeTwice() {
    final Store localStore = createRawStore();
    localStore.start();
    localStore.close();
    localStore.close(); // This should be a no-op.
  }

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

    final ImmutableMultimap.Builder<ValueType<?>, HasId> builder = ImmutableMultimap.builder();
    for (int i = 0; i < 100; ++i) {
      final int index = i % creators.size();
      final HasId obj = creators.get(index).supplier.get();
      builder.put(creators.get(index).type, obj);
    }

    final Multimap<ValueType<?>, HasId> objs = builder.build();
    objs.forEach(this::putThenLoad);

    testLoad(objs);
  }

  @Test
  void loadSteps() {
    final Multimap<ValueType<?>, HasId> objs = ImmutableMultimap.<ValueType<?>, HasId>builder()
        .put(ValueType.REF, SampleEntities.createBranch(random))
        .put(ValueType.REF, SampleEntities.createBranch(random))
        .put(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random))
        .build();

    final Multimap<ValueType<?>, HasId> objs2 = ImmutableMultimap.<ValueType<?>, HasId>builder()
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
    final Multimap<ValueType<?>, HasId> objs = ImmutableMultimap.of(ValueType.REF, SampleEntities.createBranch(random));

    Assertions.assertThrows(NotFoundException.class, () -> testLoad(objs));
  }

  @Test
  void loadSingleInvalid() {
    Assertions.assertThrows(NotFoundException.class, () -> EntityType.REF.loadSingle(store, SampleEntities.createId(random)));
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

  static class EntitySaveOp<C extends BaseValue<C>> {
    final ValueType<C> type;
    final PersistentBase<C> entity;
    final SaveOp<C> saveOp;

    EntitySaveOp(ValueType<C> type, PersistentBase<C> entity) {
      this.type = type;
      this.entity = entity;
      this.saveOp = EntityType.forType(type).createSaveOpForEntity(entity);
    }
  }

  @Test
  void save() {
    List<EntitySaveOp<?>> entities = Arrays.asList(
        new EntitySaveOp<>(ValueType.L1, SampleEntities.createL1(random)),
        new EntitySaveOp<>(ValueType.L2, SampleEntities.createL2(random)),
        new EntitySaveOp<>(ValueType.L3, SampleEntities.createL3(random)),
        new EntitySaveOp<>(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random)),
        new EntitySaveOp<>(ValueType.REF, SampleEntities.createBranch(random)),
        new EntitySaveOp<>(ValueType.REF, SampleEntities.createTag(random)),
        new EntitySaveOp<>(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random)),
        new EntitySaveOp<>(ValueType.VALUE, SampleEntities.createValue(random))
    );

    store.save(entities.stream().map(e -> e.saveOp).collect(Collectors.toList()));

    assertAll(entities.stream().map(s -> () -> {
      try {
        final HasId saveOpValue = s.entity;
        HasId loadedValue = EntityType.forType(s.type).loadSingle(store, saveOpValue.getId());
        assertEquals(saveOpValue, loadedValue, "type " + s.type);
        assertEquals(saveOpValue.getId(), loadedValue.getId(), "ID type " + s.type);

        try {
          loadedValue = EntityType.forType(s.type).buildEntity(producer -> {
            @SuppressWarnings("rawtypes") ValueType t = s.type;
            @SuppressWarnings("rawtypes") BaseValue p = producer;
            store.loadSingle(t, saveOpValue.getId(), p);
          });
          assertEquals(saveOpValue, loadedValue, "type " + s.type);
          assertEquals(saveOpValue.getId(), loadedValue.getId(), "ID type " + s.type);
        } catch (UnsupportedOperationException e) {
          // TODO ignore this for now
        }

      } catch (NotFoundException e) {
        Assertions.fail("type " + s.type, e);
      }
    }));
  }

  @Test
  void putWithConditionValue() {
    putWithCondition(ValueType.VALUE, SampleEntities.createValue(random));
  }

  @Test
  void putWithConditionBranch() {
    putWithCondition(ValueType.REF, SampleEntities.createBranch(random));
  }

  @Test
  void putWithConditionTag() {
    putWithCondition(ValueType.REF, SampleEntities.createTag(random));
  }

  @Test
  void putWithConditionCommitMetadata() {
    putWithCondition(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
  }

  @Test
  void putWithConditionKeyFragment() {
    putWithCondition(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random));
  }

  @Test
  void putWithConditionL1() {
    putWithCondition(ValueType.L1, SampleEntities.createL1(random));
  }

  @Test
  void putWithConditionL2() {
    putWithCondition(ValueType.L2, SampleEntities.createL2(random));
  }

  @Test
  void putWithConditionL3() {
    putWithCondition(ValueType.L3, SampleEntities.createL3(random));
  }

  @Test
  void deleteWithConditionValue() {
    deleteWithCondition(ValueType.VALUE, SampleEntities.createValue(random));
  }

  @Test
  void deleteWithConditionBranch() {
    deleteWithCondition(ValueType.REF, SampleEntities.createBranch(random));
  }

  @Test
  void deleteWithConditionTag() {
    deleteWithCondition(ValueType.REF, SampleEntities.createTag(random));
  }

  @Test
  void deleteWithConditionCommitMetadata() {
    deleteWithCondition(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
  }

  @Test
  void deleteConditionMismatchAttributeValue() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("value").build(),
        SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteConditional(ValueType.VALUE, SampleEntities.createValue(random), false, Optional.of(ex));
  }

  @Test
  void deleteConditionMismatchAttributeBranch() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("commit").build(),
        SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteConditional(ValueType.REF, SampleEntities.createBranch(random), false, Optional.of(ex));
  }

  @Test
  void deleteBranchSizeFail() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), ONE));
    deleteConditional(ValueType.REF, SampleEntities.createBranch(random), false, Optional.of(expression));
  }

  @Test
  void deleteBranchSizeSucceed() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), TWO));
    deleteConditional(ValueType.REF, SampleEntities.createBranch(random), true, Optional.of(expression));
  }

  @Test
  void deleteWithConditionKeyFragment() {
    deleteWithCondition(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random));
  }

  @Test
  void deleteWithConditionL1() {
    deleteWithCondition(ValueType.L1, SampleEntities.createL1(random));
  }

  @Test
  void deleteWithConditionL2() {
    deleteWithCondition(ValueType.L2, SampleEntities.createL2(random));
  }

  @Test
  void deleteWithConditionL3() {
    deleteWithCondition(ValueType.L3, SampleEntities.createL3(random));
  }

  @Test
  void deleteValue() {
    delete(ValueType.VALUE, SampleEntities.createValue(random));
  }

  @Test
  void deleteBranch() {
    delete(ValueType.REF, SampleEntities.createBranch(random));
  }

  @Test
  void deleteTag() {
    delete(ValueType.REF, SampleEntities.createTag(random));
  }

  @Test
  void deleteCommitMetadata() {
    delete(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
  }

  @Test
  void deleteKeyFragment() {
    delete(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random));
  }

  @Test
  void deleteL1() {
    delete(ValueType.L1, SampleEntities.createL1(random));
  }

  @Test
  void deleteL2() {
    delete(ValueType.L2, SampleEntities.createL2(random));
  }

  @Test
  void deleteL3() {
    delete(ValueType.L3, SampleEntities.createL3(random));
  }

  @Test
  void loadPagination() {
    final ImmutableMultimap.Builder<ValueType<?>, HasId> builder = ImmutableMultimap.builder();
    for (int i = 0; i < (10 + loadSize()); ++i) {
      // Only create a single type as this is meant to test the pagination within Mongo, not the variety. Variety is
      // taken care of by a test in AbstractTestStore.
      builder.put(ValueType.REF, SampleEntities.createTag(random));
    }

    final Multimap<ValueType<?>, HasId> objs = builder.build();
    objs.forEach(this::putThenLoad);

    testLoad(objs);
  }

  private <T extends HasId> void deleteWithCondition(ValueType type, HasId sample) {
    final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
    final ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, sample.getId().toEntity()));
    deleteConditional(type, sample, true, Optional.of(conditionExpression));
  }

  private <T extends HasId> void delete(ValueType type, HasId sample) {
    deleteConditional(type, sample, true, Optional.empty());
  }

  protected <C extends BaseValue<C>> void deleteConditional(ValueType type, HasId sample, boolean shouldSucceed,
                                                            Optional<ConditionExpression> conditionExpression) {
    store.put(new EntitySaveOp<>(type, (PersistentBase<C>) sample).saveOp, Optional.<ConditionExpression>empty());

    if (shouldSucceed) {
      Assertions.assertTrue(store.delete(type, sample.getId(), conditionExpression));
      Assertions.assertThrows(NotFoundException.class, () -> testLoadSingle(type, sample));
    } else {
      Assertions.assertFalse(store.delete(type, sample.getId(), conditionExpression));
      testLoadSingle(type, sample);
    }
  }

  private <T extends HasId> void putWithCondition(ValueType type, HasId sample) {
    // Tests that attempt to put (update) an existing entry should only occur when the condition expression is met.
    testPut(type, sample);

    final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
    final ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, sample.getId().toEntity()));
    putConditional(type, sample, true, Optional.of(conditionExpression));
  }

  protected <C extends BaseValue<C>> void putConditional(ValueType type, HasId sample, boolean shouldSucceed,
                                                  Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(new EntitySaveOp<>(type, (PersistentBase<C>) sample).saveOp, conditionExpression);
      if (!shouldSucceed) {
        Assertions.fail();
      }
      testLoadSingle(type, sample);
    } catch (ConditionFailedException cfe) {
      if (shouldSucceed) {
        Assertions.fail(cfe);
      }

      // TODO: fix sample type.
      //      Assertions.assertThrows(NotFoundException.class, () -> store.loadSingle(type, sample.getId(), sample));
    }
  }

  @SuppressWarnings("unchecked")
  private <C extends BaseValue<C>> void putThenLoad(ValueType<C> type, HasId sample) {
    store.put(new EntitySaveOp<>(type, (PersistentBase<C>) sample).saveOp, Optional.empty());
    testLoadSingle(type, sample);
  }

  protected void testLoad(Multimap<ValueType<?>, HasId> objs) {
    store.load(createTestLoadStep(objs));
  }

  protected LoadStep createTestLoadStep(Multimap<ValueType<?>, HasId> objs) {
    return createTestLoadStep(objs, Optional.empty());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected LoadStep createTestLoadStep(Multimap<ValueType<?>, HasId> objs, Optional<LoadStep> next) {
    final EntityLoadOps loadOps = new EntityLoadOps();
    objs.forEach((type, val) -> loadOps.load(((EntityType) EntityType.forType(type)), val.getId(), r -> {
      assertEquals(val, r);
      assertEquals(val.getId(), r.getId());
    }));
    return loadOps.build(() -> next);
  }

  @SuppressWarnings("unchecked")
  protected <T extends HasId> void testLoadSingle(ValueType<?> type, T sample) {
    final T read = (T) EntityType.forType(type).loadSingle(store, sample.getId());
    assertEquals(sample, read);
    assertEquals(sample.getId(), read.getId());
  }

  protected <C extends BaseValue<C>, T extends PersistentBase<C>> void testPutIfAbsent(ValueType<C> type, T sample) {
    Assertions.assertTrue(store.putIfAbsent(new EntitySaveOp<>(type, sample).saveOp));
    testLoadSingle(type, sample);
    Assertions.assertFalse(store.putIfAbsent(new EntitySaveOp<>(type, sample).saveOp));
    testLoadSingle(type, sample);
  }

  protected <C extends BaseValue<C>> void testPut(ValueType type, HasId sample) {
    testPut(type, sample, Optional.empty());
  }

  private <C extends BaseValue<C>, T extends PersistentBase<C>> void testPut(ValueType type, HasId sample,
                                                                             Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(new EntitySaveOp<>(type, (PersistentBase<C>) sample).saveOp, conditionExpression);
    } catch (ConditionFailedException e) {
      Assertions.fail(e);
    }

    testLoadSingle(type, sample);
  }
}

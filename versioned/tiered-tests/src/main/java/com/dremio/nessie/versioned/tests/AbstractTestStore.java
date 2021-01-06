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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.ConditionFailedException;
import com.dremio.nessie.versioned.store.Entity;
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
 * Common class for testing APIs of a Store.
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

  protected static final ExpressionPath COMMITS = ExpressionPath.builder("commits").build();
  protected static final Entity ONE = Entity.ofNumber(1);
  protected static final Entity TWO = Entity.ofNumber(2);

  protected Random random;
  protected S store;
  protected ImmutableList<CreatorPair> creators;

  /**
   * Create and start the store, if not already done.
   */
  @BeforeEach
  void setup() {
    if (store == null) {
      this.store = createStore();
      this.store.start();
      random = new Random(getRandomSeed());
      creators = ImmutableList.<CreatorPair>builder()
          .add(new CreatorPair(ValueType.REF, () -> SampleEntities.createTag(random)))
          .add(new CreatorPair(ValueType.REF, () -> SampleEntities.createBranch(random)))
          .add(new CreatorPair(ValueType.COMMIT_METADATA, () -> SampleEntities.createCommitMetadata(random)))
          .add(new CreatorPair(ValueType.VALUE, () -> SampleEntities.createValue(random)))
          .add(new CreatorPair(ValueType.L1, () -> SampleEntities.createL1(random)))
          .add(new CreatorPair(ValueType.L2, () -> SampleEntities.createL2(random)))
          .add(new CreatorPair(ValueType.L3, () -> SampleEntities.createL3(random)))
          .add(new CreatorPair(ValueType.KEY_FRAGMENT, () -> SampleEntities.createFragment(random)))
          .build();
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
  void closeWithoutStart() {
    final Store localStore = createStore();
    localStore.close(); // This should be a no-op.
  }

  @Test
  void closeTwice() {
    final Store localStore = createStore();
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

    final ImmutableMultimap.Builder<ValueType, HasId> builder = ImmutableMultimap.builder();
    for (int i = 0; i < 100; ++i) {
      final int index = i % creators.size();
      final HasId obj = creators.get(index).supplier.get();
      builder.put(creators.get(index).type, obj);
    }

    final Multimap<ValueType, HasId> objs = builder.build();
    objs.forEach(this::testPut);

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

    objs.forEach(this::testPut);
    objs2.forEach(this::testPut);

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
    testPut(ValueType.REF, SampleEntities.createBranch(random));
    final Multimap<ValueType, HasId> objs = ImmutableMultimap.of(ValueType.REF, SampleEntities.createBranch(random));

    Assertions.assertThrows(NotFoundException.class, () -> testLoad(objs));
  }

  @Test
  void loadSingleInvalid() {
    Assertions.assertThrows(NotFoundException.class, () -> store.loadSingle(ValueType.REF, SampleEntities.createId(random)));
  }

  @Test
  void loadSingleL1() {
    testPut(ValueType.L1, SampleEntities.createL1(random));
  }

  @Test
  void loadSingleL2() {
    testPut(ValueType.L2, SampleEntities.createL2(random));
  }

  @Test
  void loadSingleL3() {
    testPut(ValueType.L3, SampleEntities.createL3(random));
  }

  @Test
  void loadFragment() {
    testPut(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random));
  }

  @Test
  void loadBranch() {
    testPut(ValueType.REF, SampleEntities.createBranch(random));
  }

  @Test
  void loadTag() {
    testPut(ValueType.REF, SampleEntities.createTag(random));
  }

  @Test
  void loadCommitMetadata() {
    testPut(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random));
  }

  @Test
  void loadValue() {
    testPut(ValueType.VALUE, SampleEntities.createValue(random));
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
  void putTwice() {
    final L3 l3 = SampleEntities.createL3(random);
    testPut(ValueType.L3, l3, Optional.empty());
    testPut(ValueType.L3, l3, Optional.empty());
  }

  @Test
  void putWithCompoundConditionTag() {
    final InternalRef sample = SampleEntities.createTag(random);
    testPut(ValueType.REF, sample);
    final InternalRef.Type type = InternalRef.Type.TAG;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("name").build(), Entity.ofString("tagName"))
    );
    putConditional(ValueType.REF, sample, true, Optional.of(condition));
  }

  @Test
  void putWithFailingConditionExpression() {
    final InternalRef sample = SampleEntities.createBranch(random);
    final InternalRef.Type type = InternalRef.Type.BRANCH;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("commit").build(), Entity.ofString("notEqual")));
    putConditional(ValueType.REF, sample, false, Optional.of(condition));
  }

  @Test
  void putToIncorrectCollection() {
    for (ValueType type : ValueType.values()) {
      for (ValueType value : ValueType.values()) {
        if (type == value) {
          continue;
        }

        assertThrows(IllegalArgumentException.class, () -> store.put(type, creators.get(value.ordinal() + 1), Optional.empty()));
      }
    }
  }

  @Test
  void deleteNoConditionValue() {
    deleteCondition(ValueType.VALUE, SampleEntities.createValue(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionL1() {
    deleteCondition(ValueType.L1, SampleEntities.createL1(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionL2() {
    deleteCondition(ValueType.L2, SampleEntities.createL2(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionL3() {
    deleteCondition(ValueType.L3, SampleEntities.createL3(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionFragment() {
    deleteCondition(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionBranch() {
    deleteCondition(ValueType.REF, SampleEntities.createBranch(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionTag() {
    deleteCondition(ValueType.REF, SampleEntities.createTag(random), true, Optional.empty());
  }

  @Test
  void deleteNoConditionCommitMetadata() {
    deleteCondition(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random), true, Optional.empty());
  }

  @Test
  void deleteConditionMismatchAttributeValue() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("value").build(),
        SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteCondition(ValueType.VALUE, SampleEntities.createValue(random), false, Optional.of(ex));
  }

  @Test
  void deleteConditionMismatchAttributeBranch() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("commit").build(),
        SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteCondition(ValueType.REF, SampleEntities.createBranch(random), false, Optional.of(ex));
  }

  @Test
  void deleteBranchSizeFail() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), ONE));
    deleteCondition(ValueType.REF, SampleEntities.createBranch(random), false, Optional.of(expression));
  }

  @Test
  void deleteBranchSizeSucceed() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), TWO));
    deleteCondition(ValueType.REF, SampleEntities.createBranch(random), true, Optional.of(expression));
  }

  @Test
  void updateWithFailedCondition() {
    final InternalRef tag = SampleEntities.createTag(random);
    testPut(ValueType.REF, tag);
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(
        ExpressionPath.builder("name").build(), Entity.ofString("badTagName")));
    final Optional<InternalRef> updated = store.update(
        ValueType.REF,
        tag.getId(),
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("metadata").build())),
        Optional.of(expression));
    Assertions.assertFalse(updated.isPresent());
  }

  @Test
  void updateWithSuccessfulCondition() {
    final InternalRef tag = SampleEntities.createTag(random);
    testPut(ValueType.REF, tag);
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(
        ExpressionPath.builder("name").build(), Entity.ofString("tagName")));
    final Optional<InternalRef> updated = store.update(
        ValueType.REF,
        tag.getId(),
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("metadata").build())),
        Optional.of(expression));
    Assertions.assertTrue(updated.isPresent());
    final Map<String, Entity> tagMap = ValueType.REF.getSchema().itemToMap(tag, true);
    final Map<String, Entity> updatedMap = ValueType.REF.getSchema().itemToMap(updated.get(), true);
    Assertions.assertFalse(updatedMap.containsKey("metadata"));
    tagMap.remove("metadata");
    Assertions.assertEquals(tagMap, updatedMap);

    final InternalRef read = store.loadSingle(ValueType.REF, tag.getId());
    Assertions.assertEquals(ValueType.REF.getSchema().itemToMap(read, true), updatedMap);
  }

  @Test
  void updateRemoveOneArray() {
    updateRemoveArray(UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("keys").position(0).build())), 1, 10);
  }

  @Test
  void updateRemoveOneArrayEnd() {
    updateRemoveArray(UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("keys").position(9).build())), 0, 9);
  }

  @Test
  void updateRemoveMultipleArrayAscending() {
    UpdateExpression update = UpdateExpression.of();
    for (int i = 0; i < 5; ++i) {
      update = update.and(RemoveClause.of(ExpressionPath.builder("keys").position(i).build()));
    }

    updateRemoveArray(update, 5, 10);
  }

  @Test
  void updateRemoveMultipleArrayDescending() {
    UpdateExpression update = UpdateExpression.of();
    for (int i = 4; i >= 0; --i) {
      update = update.and(RemoveClause.of(ExpressionPath.builder("keys").position(i).build()));
    }

    updateRemoveArray(update, 5, 10);
  }

  @Test
  void updateSetEquals() {
    final String tagName = "myTag";
    final InternalRef tag = SampleEntities.createTag(random);
    testPut(ValueType.REF, tag);
    final Optional<InternalRef> updated = store.update(
        ValueType.REF,
        tag.getId(),
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder("name").build(), Entity.ofString(tagName))),
        Optional.empty());
    Assertions.assertTrue(updated.isPresent());
    final Map<String, Entity> tagMap = ValueType.REF.getSchema().itemToMap(tag, true);
    final Map<String, Entity> updatedMap = ValueType.REF.getSchema().itemToMap(updated.get(), true);

    final InternalRef read = store.loadSingle(ValueType.REF, tag.getId());
    Assertions.assertEquals(ValueType.REF.getSchema().itemToMap(read, true), updatedMap);

    Assertions.assertEquals(tagName, updatedMap.get("name").getString());
    updatedMap.remove("name");
    tagMap.remove("name");
    Assertions.assertEquals(tagMap, updatedMap);
  }

  @Test
  void updateSetListAppend() {
    final String key = "newKey";
    final Fragment fragment = SampleEntities.createFragment(random);
    testPut(ValueType.KEY_FRAGMENT, fragment);
    final Optional<InternalRef> updated = store.update(
        ValueType.KEY_FRAGMENT,
        fragment.getId(),
        UpdateExpression.of(SetClause.appendToList(ExpressionPath.builder("keys").build(), Entity.ofList(Entity.ofString(key)))),
        Optional.empty());
    Assertions.assertTrue(updated.isPresent());

    final Map<String, Entity> oldMap = ValueType.KEY_FRAGMENT.getSchema().itemToMap(fragment, true);
    final List<Entity> oldKeys = new ArrayList<>(oldMap.get("keys").getList());
    oldKeys.add(Entity.ofList(Entity.ofString(key)));
    final Map<String, Entity> updatedMap = ValueType.KEY_FRAGMENT.getSchema().itemToMap(updated.get(), true);
    Assertions.assertEquals(oldKeys, updatedMap.get("keys").getList());

    final Fragment read = store.loadSingle(ValueType.KEY_FRAGMENT, fragment.getId());
    Assertions.assertEquals(ValueType.KEY_FRAGMENT.getSchema().itemToMap(read, true), updatedMap);
  }

  private <T extends HasId> void putWithCondition(ValueType type, T sample) {
    // Tests that attempt to put (update) an existing entry should only occur when the condition expression is met.
    testPut(type, sample);

    final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
    final ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, sample.getId().toEntity()));
    putConditional(type, sample, true, Optional.of(conditionExpression));
  }

  protected <T extends HasId> void deleteCondition(ValueType type, T sample, boolean shouldSucceed,
                                                   Optional<ConditionExpression> conditionExpression) {
    testPut(type, sample, Optional.empty());
    if (shouldSucceed) {
      Assertions.assertTrue(store.delete(type, sample.getId(), conditionExpression));
      Assertions.assertThrows(NotFoundException.class, () -> store.loadSingle(type, sample.getId()));
    } else {
      Assertions.assertFalse(store.delete(type, sample.getId(), conditionExpression));
      testLoadSingle(type, sample);
    }
  }

  protected <T extends HasId> void putConditional(ValueType type, T sample, boolean shouldSucceed,
                                                  Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(type, sample, conditionExpression);
      if (!shouldSucceed) {
        Assertions.fail();
      }
      testLoadSingle(type, sample);
    } catch (ConditionFailedException cfe) {
      if (shouldSucceed) {
        Assertions.fail(cfe);
      }

      Assertions.assertThrows(NotFoundException.class, () -> store.loadSingle(type, sample.getId()));
    }
  }

  private <T extends HasId> void testLoadSingle(ValueType type, T sample) {
    try {
      final T read = store.loadSingle(type, sample.getId());
      final SimpleSchema<T> schema = type.getSchema();
      assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }

  protected <T extends HasId> void testPut(ValueType type, T sample) {
    testPut(type, sample, Optional.empty());
  }

  private <T extends HasId> void testPut(ValueType type, T sample, Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(type, sample, conditionExpression);
    } catch (ConditionFailedException e) {
      Assertions.fail(e);
    }

    testLoadSingle(type, sample);
  }

  private void updateRemoveArray(UpdateExpression update, int beginArrayIndex, int endArrayIndex) {
    final Fragment fragment = SampleEntities.createFragment(random);
    testPut(ValueType.KEY_FRAGMENT, fragment);
    final Optional<InternalRef> updated = store.update(
        ValueType.KEY_FRAGMENT,
        fragment.getId(),
        update,
        Optional.empty());
    Assertions.assertTrue(updated.isPresent());

    final Map<String, Entity> oldMap = ValueType.KEY_FRAGMENT.getSchema().itemToMap(fragment, true);
    final List<Entity> oldKeys = oldMap.get("keys").getList().subList(beginArrayIndex, endArrayIndex);
    final Map<String, Entity> updatedMap = ValueType.KEY_FRAGMENT.getSchema().itemToMap(updated.get(), true);
    Assertions.assertEquals(oldKeys, updatedMap.get("keys").getList());

    final Fragment read = store.loadSingle(ValueType.KEY_FRAGMENT, fragment.getId());
    Assertions.assertEquals(ValueType.KEY_FRAGMENT.getSchema().itemToMap(read, true), updatedMap);
  }

  protected void testLoad(Multimap<ValueType, HasId> objs) {
    store.load(createTestLoadStep(objs));
  }

  protected LoadStep createTestLoadStep(Multimap<ValueType, HasId> objs) {
    return createTestLoadStep(objs, Optional.empty());
  }

  protected LoadStep createTestLoadStep(Multimap<ValueType, HasId> objs, Optional<LoadStep> next) {
    return new LoadStep(
        objs.entries().stream().map(e -> new LoadOp<>(e.getKey(), e.getValue().getId(),
            r -> assertEquals(e.getKey().getSchema().itemToMap(e.getValue(), true),
                e.getKey().getSchema().itemToMap(r, true)))
        ).collect(Collectors.toList()),
        () -> next
    );
  }

  protected <T extends HasId> void testPutIfAbsent(ValueType type, T sample) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    testLoadSingle(type, sample);
    Assertions.assertFalse(store.putIfAbsent(type, sample));
    testLoadSingle(type, sample);
  }
}

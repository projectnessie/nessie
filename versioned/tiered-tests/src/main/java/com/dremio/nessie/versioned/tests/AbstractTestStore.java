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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.impl.condition.RemoveClause;
import com.dremio.nessie.versioned.impl.condition.SetClause;
import com.dremio.nessie.versioned.impl.condition.UpdateExpression;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.NotFoundException;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;

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

  protected abstract S createStore();

  protected abstract long getRandomSeed();

  protected abstract void resetStoreState();

  @Test
  void loadSingleL1() {
    putThenLoad(SampleEntities.createL1(random), ValueType.L1);
  }

  @Test
  void loadSingleL2() {
    putThenLoad(SampleEntities.createL2(random), ValueType.L2);
  }

  @Test
  void loadSingleL3() {
    putThenLoad(SampleEntities.createL3(random), ValueType.L3);
  }

  @Test
  void loadFragment() {
    putThenLoad(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT);
  }

  @Test
  void loadBranch() {
    putThenLoad(SampleEntities.createBranch(random), ValueType.REF);
  }

  @Test
  void loadTag() {
    putThenLoad(SampleEntities.createTag(random), ValueType.REF);
  }

  @Test
  void loadCommitMetadata() {
    putThenLoad(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA);
  }

  @Test
  void loadValue() {
    putThenLoad(SampleEntities.createValue(random), ValueType.VALUE);
  }

  @Test
  void putIfAbsentL1() {
    testPutIfAbsent(SampleEntities.createL1(random), ValueType.L1);
  }

  @Test
  void putIfAbsentL2() {
    testPutIfAbsent(SampleEntities.createL2(random), ValueType.L2);
  }

  @Test
  void putIfAbsentL3() {
    testPutIfAbsent(SampleEntities.createL3(random), ValueType.L3);
  }

  @Test
  void putIfAbsentFragment() {
    testPutIfAbsent(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT);
  }

  @Test
  void putIfAbsentBranch() {
    testPutIfAbsent(SampleEntities.createBranch(random), ValueType.REF);
  }

  @Test
  void putIfAbsentTag() {
    testPutIfAbsent(SampleEntities.createTag(random), ValueType.REF);
  }

  @Test
  void putIfAbsentCommitMetadata() {
    testPutIfAbsent(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA);
  }

  @Test
  void putIfAbsentValue() {
    testPutIfAbsent(SampleEntities.createValue(random), ValueType.VALUE);
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
    putWithCondition(SampleEntities.createValue(random), ValueType.VALUE);
  }

  @Test
  void putWithConditionBranch() {
    putWithCondition(SampleEntities.createBranch(random), ValueType.REF);
  }

  @Test
  void putWithConditionTag() {
    putWithCondition(SampleEntities.createTag(random), ValueType.REF);
  }

  @Test
  void putWithConditionCommitMetadata() {
    putWithCondition(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA);
  }

  @Test
  void putWithConditionKeyFragment() {
    putWithCondition(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT);
  }

  @Test
  void putWithConditionL1() {
    putWithCondition(SampleEntities.createL1(random), ValueType.L1);
  }

  @Test
  void putWithConditionL2() {
    putWithCondition(SampleEntities.createL2(random), ValueType.L2);
  }

  @Test
  void putWithConditionL3() {
    putWithCondition(SampleEntities.createL3(random), ValueType.L3);
  }

  @Test
  void putWithCompoundConditionTag() {
    final InternalRef sample = SampleEntities.createTag(random);
    putThenLoad(sample, ValueType.REF);
    final Id id = sample.getId();
    final InternalRef.Type type = InternalRef.Type.TAG;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("commit").build(), id.toEntity())
    );
    putConditional(sample, ValueType.REF, true, Optional.of(condition));
  }

  @Test
  void putWithFailingConditionExpression() {
    final InternalRef sample = SampleEntities.createBranch(random);
    final InternalRef.Type type = InternalRef.Type.BRANCH;
    final ConditionExpression condition = ConditionExpression.of(
        ExpressionFunction.equals(ExpressionPath.builder(InternalRef.TYPE).build(), type.toEntity()),
        ExpressionFunction.equals(ExpressionPath.builder("commit").build(), Entity.ofString("notEqual")));
    putConditional(sample, ValueType.REF, false, Optional.of(condition));
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
    deleteCondition(SampleEntities.createValue(random), ValueType.VALUE, true, Optional.empty());
  }

  @Test
  void deleteNoConditionL1() {
    deleteCondition(SampleEntities.createL1(random), ValueType.L1, true, Optional.empty());
  }

  @Test
  void deleteNoConditionL2() {
    deleteCondition(SampleEntities.createL2(random), ValueType.L2, true, Optional.empty());
  }

  @Test
  void deleteNoConditionL3() {
    deleteCondition(SampleEntities.createL3(random), ValueType.L3, true, Optional.empty());
  }

  @Test
  void deleteNoConditionFragment() {
    deleteCondition(SampleEntities.createFragment(random), ValueType.KEY_FRAGMENT, true, Optional.empty());
  }

  @Test
  void deleteNoConditionBranch() {
    deleteCondition(SampleEntities.createBranch(random), ValueType.REF, true, Optional.empty());
  }

  @Test
  void deleteNoConditionTag() {
    deleteCondition(SampleEntities.createTag(random), ValueType.REF, true, Optional.empty());
  }

  @Test
  void deleteNoConditionCommitMetadata() {
    deleteCondition(SampleEntities.createCommitMetadata(random), ValueType.COMMIT_METADATA, true, Optional.empty());
  }

  @Test
  void deleteConditionMismatchAttributeValue() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("value").build(),
        SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteCondition(SampleEntities.createValue(random), ValueType.VALUE, false, Optional.of(ex));
  }

  @Test
  void deleteConditionMismatchAttributeBranch() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("commit").build(),
        SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
    final ConditionExpression ex = ConditionExpression.of(expressionFunction);
    deleteCondition(SampleEntities.createBranch(random), ValueType.REF, false, Optional.of(ex));
  }

  @Test
  void deleteBranchSizeFail() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), ONE));
    deleteCondition(SampleEntities.createBranch(random), ValueType.REF, false, Optional.of(expression));
  }

  @Test
  void deleteBranchSizeSucceed() {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), TWO));
    deleteCondition(SampleEntities.createBranch(random), ValueType.REF, true, Optional.of(expression));
  }

  @Test
  void updateWithFailedCondition() {
    final InternalRef tag = SampleEntities.createTag(random);
    putThenLoad(tag, ValueType.REF);
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
    putThenLoad(tag, ValueType.REF);
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
  }

  @Test
  void updateRemove() {
    final Fragment fragment = SampleEntities.createFragment(random);
    putThenLoad(fragment, ValueType.KEY_FRAGMENT);
    final Optional<InternalRef> updated = store.update(
        ValueType.KEY_FRAGMENT,
        fragment.getId(),
        UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("keys").position(0).build())),
        Optional.empty());
    Assertions.assertTrue(updated.isPresent());

    final Map<String, Entity> oldMap = ValueType.KEY_FRAGMENT.getSchema().itemToMap(fragment, true);
    final List<Entity> oldKeys = oldMap.get("keys").getList().subList(1, 10);
    final Map<String, Entity> updatedMap = ValueType.KEY_FRAGMENT.getSchema().itemToMap(updated.get(), true);
    Assertions.assertEquals(oldKeys, updatedMap.get("keys").getList());
  }

  @Test
  void updateSetEquals() {
    final String tagName = "myTag";
    final InternalRef tag = SampleEntities.createTag(random);
    putThenLoad(tag, ValueType.REF);
    final Optional<InternalRef> updated = store.update(
        ValueType.REF,
        tag.getId(),
        UpdateExpression.of(SetClause.equals(ExpressionPath.builder("name").build(), Entity.ofString(tagName))),
        Optional.empty());
    Assertions.assertTrue(updated.isPresent());
    final Map<String, Entity> tagMap = ValueType.REF.getSchema().itemToMap(tag, true);
    final Map<String, Entity> updatedMap = ValueType.REF.getSchema().itemToMap(updated.get(), true);
    Assertions.assertEquals(tagName, updatedMap.get("name").getString());
    updatedMap.remove("name");
    tagMap.remove("name");
    Assertions.assertEquals(tagMap, updatedMap);
  }

  @Test
  void updateSetListAppend() {
    final String key = "newKey";
    final Fragment fragment = SampleEntities.createFragment(random);
    putThenLoad(fragment, ValueType.KEY_FRAGMENT);
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
  }

  private <T extends HasId> void putWithCondition(T sample, ValueType type) {
    // Tests that attempt to put (update) an existing entry should only occur when the condition expression is met.
    putThenLoad(sample, type);

    final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
    final ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, sample.getId().toEntity()));
    putConditional(sample, type, true, Optional.of(conditionExpression));
  }

  protected <T extends HasId> void deleteCondition(T sample, ValueType type, boolean shouldSucceed,
                                                   Optional<ConditionExpression> conditionExpression) {
    testPut(sample, type, Optional.empty());
    if (shouldSucceed) {
      Assertions.assertTrue(store.delete(type, sample.getId(), conditionExpression));
      testNotLoaded(sample, type);
    } else {
      Assertions.assertFalse(store.delete(type, sample.getId(), conditionExpression));
      testLoadSingle(sample, type);
    }
  }

  protected <T extends HasId> void putConditional(T sample, ValueType type, boolean shouldSucceed,
                                                  Optional<ConditionExpression> conditionExpression) {
    store.put(type, sample, conditionExpression);
    if (shouldSucceed) {
      testLoadSingle(sample, type);
    } else {
      testNotLoaded(sample, type);
    }
  }

  private <T extends HasId> void putThenLoad(T sample, ValueType type) {
    try {
      store.put(type, sample, Optional.empty());
      testLoadSingle(sample, type);
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }

  private <T extends HasId> void testLoadSingle(T sample, ValueType type) {
    try {
      final T read = store.loadSingle(type, sample.getId());
      final SimpleSchema<T> schema = type.getSchema();
      assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }

  private <T extends HasId> void testNotLoaded(T sample, ValueType type) {
    Assertions.assertThrows(NotFoundException.class, () -> store.loadSingle(type, sample.getId()));
  }

  private <T extends HasId> void testPutIfAbsent(T sample, ValueType type) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    testLoadSingle(sample, type);
    Assertions.assertFalse(store.putIfAbsent(type, sample));
    testLoadSingle(sample, type);
  }

  private <T extends HasId> void testPut(T sample, ValueType type, Optional<ConditionExpression> conditionExpression) {
    try {
      store.put(type, sample, conditionExpression);
      testLoadSingle(sample, type);
    } catch (NotFoundException e) {
      Assertions.fail(e);
    }
  }
}

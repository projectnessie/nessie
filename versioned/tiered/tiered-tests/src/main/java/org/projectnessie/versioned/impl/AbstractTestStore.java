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
package org.projectnessie.versioned.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.ExpressionFunction;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.impl.condition.RemoveClause;
import org.projectnessie.versioned.impl.condition.SetClause;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.HasId;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * Common class for testing public APIs of a Store.
 * This class should be moved to the versioned/tests project when it will not introduce a circular dependency.
 * @param <S> The type of the Store being tested.
 */
public abstract class AbstractTestStore<S extends Store> {

  static class CreatorPair {
    final ValueType<?> type;
    final Supplier<PersistentBase<?>> supplier;

    CreatorPair(ValueType<?> type, Supplier<PersistentBase<?>> supplier) {
      this.type = type;
      this.supplier = supplier;
    }
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

  protected static final ExpressionPath COMMITS = ExpressionPath.builder("commits").build();
  protected static final Entity ONE = Entity.ofNumber(1);
  protected static final Entity TWO = Entity.ofNumber(2);

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

  protected abstract S createRawStore();

  protected abstract long getRandomSeed();

  protected abstract void resetStoreState();

  protected abstract int loadSize();

  protected boolean supportsDelete() {
    return true;
  }

  protected boolean supportsUpdate() {
    return true;
  }

  protected boolean supportsConditionExpression() {
    return true;
  }

  // Tests for close()

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

  @Nested
  @DisplayName("load() tests")
  class LoadTests {
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
      objs.forEach(AbstractTestStore.this::putThenLoad);

      store.load(createTestLoadStep(objs));
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

      objs.forEach(AbstractTestStore.this::putThenLoad);
      objs2.forEach(AbstractTestStore.this::putThenLoad);

      final LoadStep step2 = createTestLoadStep(objs2);
      final LoadStep step1 = createTestLoadStep(objs, Optional.of(step2));

      store.load(step1);
    }

    @Test
    void loadNone() {
      store.load(createTestLoadStep(ImmutableMultimap.of()));
    }

    @Test
    void loadInvalid() {
      putThenLoad(ValueType.REF, SampleEntities.createBranch(random));
      final Multimap<ValueType<?>, HasId> objs = ImmutableMultimap.of(ValueType.REF, SampleEntities.createBranch(random));

      Assertions.assertThrows(NotFoundException.class, () -> store.load(createTestLoadStep(objs)));
    }

    @Test
    void loadPagination() {
      final ImmutableMultimap.Builder<ValueType<?>, HasId> builder = ImmutableMultimap.builder();
      for (int i = 0; i < (10 + loadSize()); ++i) {
        // Only create a single type as this is meant to test the pagination within the store, not the variety. Variety is
        // taken care of by another test.
        builder.put(ValueType.REF, SampleEntities.createTag(random));
      }

      final Multimap<ValueType<?>, HasId> objs = builder.build();
      objs.forEach(AbstractTestStore.this::putThenLoad);

      store.load(createTestLoadStep(objs));
    }

    private LoadStep createTestLoadStep(Multimap<ValueType<?>, HasId> objs) {
      return createTestLoadStep(objs, Optional.empty());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private LoadStep createTestLoadStep(Multimap<ValueType<?>, HasId> objs, Optional<LoadStep> next) {
      final EntityLoadOps loadOps = new EntityLoadOps();
      objs.forEach((type, val) -> loadOps.load(((EntityType) EntityType.forType(type)), val.getId(), r -> assertEquals(val, r)));
      return loadOps.build(() -> next);
    }
  }

  @Nested
  @DisplayName("loadSingle() tests")
  class LoadSingleTests {
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
  }

  @Nested
  @DisplayName("putIfAbsent() tests")
  class PutIfAbsentTests {
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

    protected <C extends BaseValue<C>, T extends PersistentBase<C>> void testPutIfAbsent(ValueType<C> type, T sample) {
      Assertions.assertTrue(store.putIfAbsent(new EntitySaveOp<>(type, sample).saveOp));
      testLoadSingle(type, sample);
      Assertions.assertFalse(store.putIfAbsent(new EntitySaveOp<>(type, sample).saveOp));
      testLoadSingle(type, sample);
    }
  }

  @Nested
  @DisplayName("save() tests")
  class SaveTests {
    @Test
    void save() {
      final List<EntitySaveOp<?>> entities = Arrays.asList(
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

      Assertions.assertAll(entities.stream().map(s -> () -> {
        try {
          final HasId saveOpValue = s.entity;
          HasId loadedValue = EntityType.forType(s.type).loadSingle(store, saveOpValue.getId());
          Assertions.assertEquals(saveOpValue, loadedValue, "type " + s.type);

          try {
            loadedValue = EntityType.forType(s.type).buildEntity(producer -> {
              @SuppressWarnings("rawtypes") ValueType t = s.type;
              @SuppressWarnings("rawtypes") BaseValue p = producer;
              store.loadSingle(t, saveOpValue.getId(), p);
            });
            Assertions.assertEquals(saveOpValue, loadedValue, "type " + s.type);
          } catch (UnsupportedOperationException e) {
            // TODO ignore this for now
          }

        } catch (NotFoundException e) {
          Assertions.fail("type " + s.type, e);
        }
      }));
    }
  }

  @Nested
  @DisplayName("put() tests")
  class PutWithoutConditionExpressionTests {
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
      final HasId l3 = SampleEntities.createL3(random);
      putThenLoad(ValueType.L3, l3);
      putThenLoad(ValueType.L3, l3);
    }

    @Test
    void putWithCompoundConditionTag() {
      final InternalRef sample = SampleEntities.createTag(random);
      putThenLoad(ValueType.REF, sample);
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

    private <T extends HasId> void putWithCondition(ValueType<?> type, T sample) {
      // Tests that attempt to put (update) an existing entry should only occur when the condition expression is met.
      putThenLoad(type, sample);

      final ExpressionPath keyName = ExpressionPath.builder(Store.KEY_NAME).build();
      final ConditionExpression conditionExpression = ConditionExpression.of(ExpressionFunction.equals(keyName, sample.getId().toEntity()));
      putConditional(type, sample, true, Optional.of(conditionExpression));
    }

    @SuppressWarnings("unchecked")
    private <C extends BaseValue<C>> void putConditional(ValueType<C> type, HasId sample, boolean shouldSucceed,
                                                           Optional<ConditionExpression> conditionExpression) {
      if (!supportsConditionExpression()) {
        return;
      }

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

        Assertions.assertThrows(NotFoundException.class, () -> EntityType.forType(type).loadSingle(store, sample.getId()));
      }
    }
  }

  @Nested
  @DisplayName("delete() tests")
  class DeleteTests {
    @Test
    void deleteNoConditionValue() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.VALUE, SampleEntities.createValue(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionL1() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.L1, SampleEntities.createL1(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionL2() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.L2, SampleEntities.createL2(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionL3() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.L3, SampleEntities.createL3(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionFragment() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.KEY_FRAGMENT, SampleEntities.createFragment(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionBranch() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.REF, SampleEntities.createBranch(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionTag() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.REF, SampleEntities.createTag(random), true, Optional.empty());
    }

    @Test
    void deleteNoConditionCommitMetadata() {
      if (!supportsDelete()) {
        return;
      }

      deleteWithCondition(ValueType.COMMIT_METADATA, SampleEntities.createCommitMetadata(random), true, Optional.empty());
    }

    @Test
    void deleteConditionMismatchAttributeValue() {
      if (!supportsDelete() || !supportsConditionExpression()) {
        return;
      }

      final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("value").build(),
          SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
      final ConditionExpression ex = ConditionExpression.of(expressionFunction);
      deleteWithCondition(ValueType.VALUE, SampleEntities.createValue(random), false, Optional.of(ex));
    }

    @Test
    void deleteConditionMismatchAttributeBranch() {
      if (!supportsDelete() || !supportsConditionExpression()) {
        return;
      }

      final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("commit").build(),
          SampleEntities.createStringEntity(random, random.nextInt(10) + 1));
      final ConditionExpression ex = ConditionExpression.of(expressionFunction);
      deleteWithCondition(ValueType.REF, SampleEntities.createBranch(random), false, Optional.of(ex));
    }

    @Test
    void deleteBranchSizeFail() {
      if (!supportsDelete() || !supportsConditionExpression()) {
        return;
      }

      final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), ONE));
      deleteWithCondition(ValueType.REF, SampleEntities.createBranch(random), false, Optional.of(expression));
    }

    @Test
    void deleteBranchSizeSucceed() {
      if (!supportsDelete() || !supportsConditionExpression()) {
        return;
      }

      final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(COMMITS), TWO));
      deleteWithCondition(ValueType.REF, SampleEntities.createBranch(random), true, Optional.of(expression));
    }

    protected <T extends HasId> void deleteWithCondition(ValueType<?> type, T sample, boolean shouldSucceed,
                                                         Optional<ConditionExpression> conditionExpression) {
      putThenLoad(type, sample);
      if (shouldSucceed) {
        Assertions.assertTrue(store.delete(type, sample.getId(), conditionExpression));
        Assertions.assertThrows(NotFoundException.class, () -> EntityType.forType(type).loadSingle(store, sample.getId()));
      } else {
        Assertions.assertFalse(store.delete(type, sample.getId(), conditionExpression));
        testLoadSingle(type, sample);
      }
    }
  }

  @Nested
  @DisplayName("update() tests")
  class UpdateTests {

    @Test
    void updateWithFailedCondition() {
      if (!supportsUpdate() || !supportsConditionExpression()) {
        return;
      }

      final InternalRef tag = SampleEntities.createTag(random);
      putThenLoad(ValueType.REF, tag);
      final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(
          ExpressionPath.builder("name").build(), Entity.ofString("badTagName")));
      Assertions.assertFalse(store.update(
          ValueType.REF,
          tag.getId(),
          UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("metadata").build())),
          Optional.of(expression),
          Optional.empty()));
    }

    @Test
    void updateWithSuccessfulCondition() {
      if (!supportsUpdate() || !supportsConditionExpression()) {
        return;
      }

      final String tagName = "myTag";
      final InternalTag tag = SampleEntities.createTag(random).getTag();
      putThenLoad(ValueType.REF, tag);
      final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(
          ExpressionPath.builder("name").build(), Entity.ofString("tagName")));
      final InternalRef.Builder<?> builder = EntityType.REF.newEntityProducer();
      final boolean result = store.update(
          ValueType.REF,
          tag.getId(),
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder("name").build(), Entity.ofString(tagName))),
          Optional.of(expression),
          Optional.of(builder));
      Assertions.assertTrue(result);
      final InternalTag updated = builder.build().getTag();

      Assertions.assertEquals(tag.getId(), updated.getId());
      Assertions.assertEquals(tag.getCommit(), updated.getCommit());
      Assertions.assertEquals(tag.getDt(), updated.getDt());
      Assertions.assertNotEquals(tag.getName(), updated.getName());
      Assertions.assertEquals(tagName, updated.getName());

      testLoadSingle(ValueType.REF, updated);
    }

    @Test
    protected void updateRemoveOneArray() {
      updateRemoveArray(UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("keys").position(0).build())), 1, 10);
    }

    @Test
    protected void updateRemoveOneArrayEnd() {
      updateRemoveArray(UpdateExpression.of(RemoveClause.of(ExpressionPath.builder("keys").position(9).build())), 0, 9);
    }

    @Test
    protected void updateRemoveMultipleArrayAscending() {
      UpdateExpression update = UpdateExpression.of();
      for (int i = 0; i < 5; ++i) {
        update = update.and(RemoveClause.of(ExpressionPath.builder("keys").position(i).build()));
      }

      updateRemoveArray(update, 5, 10);
    }

    @Test
    protected void updateRemoveMultipleArrayDescending() {
      UpdateExpression update = UpdateExpression.of();
      for (int i = 4; i >= 0; --i) {
        update = update.and(RemoveClause.of(ExpressionPath.builder("keys").position(i).build()));
      }

      updateRemoveArray(update, 5, 10);
    }

    @Test
    void updateSetEquals() {
      if (!supportsUpdate()) {
        return;
      }

      final String tagName = "myTag";
      final InternalTag tag = SampleEntities.createTag(random).getTag();
      putThenLoad(ValueType.REF, tag);
      final InternalRef.Builder<?> builder = EntityType.REF.newEntityProducer();
      final boolean result = store.update(
          ValueType.REF,
          tag.getId(),
          UpdateExpression.of(SetClause.equals(ExpressionPath.builder("name").build(), Entity.ofString(tagName))),
          Optional.empty(),
          Optional.of(builder));
      Assertions.assertTrue(result);
      final InternalTag updated = builder.build().getTag();

      Assertions.assertEquals(tag.getId(), updated.getId());
      Assertions.assertEquals(tag.getCommit(), updated.getCommit());
      Assertions.assertEquals(tag.getDt(), updated.getDt());
      Assertions.assertNotEquals(tag.getName(), updated.getName());
      Assertions.assertEquals(tagName, updated.getName());

      testLoadSingle(ValueType.REF, updated);
    }

    @Test
    void updateSetListAppend() {
      if (!supportsUpdate()) {
        return;
      }

      final String key = "newKey";
      final InternalFragment fragment = SampleEntities.createFragment(random);
      putThenLoad(ValueType.KEY_FRAGMENT, fragment);
      final InternalFragment.Builder builder = EntityType.KEY_FRAGMENT.newEntityProducer();
      final boolean result = store.update(
          ValueType.KEY_FRAGMENT,
          fragment.getId(),
          UpdateExpression.of(SetClause.appendToList(
              ExpressionPath.builder("keys").build(), Entity.ofList(Entity.ofList(Entity.ofString(key))))),
          Optional.empty(),
          Optional.of(builder));
      Assertions.assertTrue(result);
      final InternalFragment updated = builder.build();

      Assertions.assertEquals(fragment.getId(), updated.getId());

      final List<InternalKey> oldKeys = new ArrayList<>(fragment.getKeys());
      oldKeys.add(InternalKey.fromEntity(Entity.ofList(Entity.ofString(key))));
      Assertions.assertEquals(oldKeys, updated.getKeys());

      testLoadSingle(ValueType.KEY_FRAGMENT, updated);
    }

    private void updateRemoveArray(UpdateExpression update, int beginArrayIndex, int endArrayIndex) {
      if (!supportsUpdate()) {
        return;
      }

      final InternalFragment fragment = SampleEntities.createFragment(random);
      putThenLoad(ValueType.KEY_FRAGMENT, fragment);
      final InternalFragment.Builder builder = EntityType.KEY_FRAGMENT.newEntityProducer();
      final boolean result = store.update(
          ValueType.KEY_FRAGMENT,
          fragment.getId(),
          update,
          Optional.empty(),
          Optional.of(builder));
      Assertions.assertTrue(result);
      final InternalFragment updated = builder.build();

      Assertions.assertEquals(fragment.getId(), updated.getId());

      final List<InternalKey> oldKeys = fragment.getKeys().subList(beginArrayIndex, endArrayIndex);
      Assertions.assertEquals(oldKeys, updated.getKeys());

      testLoadSingle(ValueType.KEY_FRAGMENT, updated);
    }
  }

  // Utility functions for the tests

  @SuppressWarnings("unchecked")
  private <C extends BaseValue<C>> void putThenLoad(ValueType<C> type, HasId sample) {
    store.put(new EntitySaveOp<>(type, (PersistentBase<C>) sample).saveOp, Optional.empty());
    testLoadSingle(type, sample);
  }

  @SuppressWarnings("unchecked")
  protected <T extends HasId> void testLoadSingle(ValueType<?> type, T sample) {
    final T read = (T) EntityType.forType(type).loadSingle(store, sample.getId());
    assertEquals(sample, read);
  }

  protected static void assertEquals(HasId expected, HasId actual) {
    Assertions.assertEquals(expected, actual);
    Assertions.assertEquals(expected.getId(), actual.getId());
  }
}

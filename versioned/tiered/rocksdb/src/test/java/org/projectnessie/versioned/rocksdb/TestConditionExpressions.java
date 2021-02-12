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
package org.projectnessie.versioned.rocksdb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Store;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

class TestConditionExpressions {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity ONE = Entity.ofString("one");
  private static final Entity TWO = Entity.ofString("two");
  private static final Entity THREE = Entity.ofString("three");
  private static final Entity NUM_ONE = Entity.ofNumber(1);
  private static final Entity NUM_FOUR = Entity.ofNumber(4);

  private static final Entity TRUE_ENTITY = Entity.ofBoolean(true);
  private static final Entity FALSE_ENTITY = Entity.ofBoolean(false);
  private static final Entity LIST_ENTITY = Entity.ofList(ONE, TWO, THREE);
  private static final Entity MAP_ENTITY = Entity.ofMap(ImmutableMap.of("key_one", ONE, "key_two", TWO, "key_three", THREE));

  // Single ExpressionFunction equals tests
  @Test
  void equalsBooleanTrue() {
    equalsEntity(TRUE_ENTITY);
  }

  @Test
  void equalsBooleanFalse() {
    equalsEntity(FALSE_ENTITY);
  }

  @Test
  void equalsList() {
    equalsEntity(LIST_ENTITY);
  }

  @Test
  void equalsMap() {
    equalsEntity(MAP_ENTITY);
  }

  @Test
  void equalsNumber() {
    final Entity numEntity = Entity.ofNumber(RANDOM.nextLong());
    equalsEntity(numEntity);
  }

  @Test
  void equalsString() {
    final Entity strEntity = SampleEntities.createStringEntity(RANDOM, 7);
    equalsEntity(strEntity);
  }

  @Test
  void equalsBinary() {
    final Entity binaryEntity = Entity.ofBinary(SampleEntities.createBinary(RANDOM, 15));
    equalsEntity(binaryEntity);
  }

  @Test
  void binaryEquals() {
    final Entity id = SampleEntities.createId(RANDOM).toEntity();
    equalsEntity(id, Store.KEY_NAME);
  }


  // Single ExpressionFunction array equals tests
  @Test
  void arraySubpathEqualsBooleanTrue() {
    final String path = createPathPos();
    equalsEntity(TRUE_ENTITY, path);
  }

  @Test
  void arraySubpathEqualsBooleanFalse() {
    final String path = createPathPos();
    equalsEntity(FALSE_ENTITY, path);
  }

  @Test
  void arraySubpathEqualsList() {
    final String path = createPathPos();
    equalsEntity(LIST_ENTITY, path);
  }

  @Test
  void arraySubpathEqualsMap() {
    final String path = createPathPos();
    equalsEntity(MAP_ENTITY, path);
  }

  @Test
  void arraySubpathEqualsNumber() {
    final String path = createPathPos();
    final Entity numEntity = Entity.ofNumber(RANDOM.nextLong());
    equalsEntity(numEntity, path);
  }

  @Test
  void arraySubpathEqualsString() {
    final String path = createPathPos();
    equalsEntity(THREE, path);
  }

  @Test
  void arraySubpathWithBinary() {
    final String path = createPathPos();
    final Entity binaryEntity = Entity.ofBinary(SampleEntities.createBinary(RANDOM, 8));
    equalsEntity(binaryEntity, path);
  }

  // Single ExpressionFunction size tests
  @Test
  void size() {
    final String path = createPath();
    final ConditionExpression ex =
        ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(ofPath(path)), NUM_FOUR));

    final List<Function> expectedCondition = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.SIZE)
          .path(ofPath(path))
          .value(NUM_FOUR)
          .build());
    equals(expectedCondition, ex);
  }

  @Test
  void equalsAndSize() {
    final String path = SampleEntities.createString(RANDOM, RANDOM.nextInt(5) + 1) + "."
        + RANDOM.nextInt(10) + "." + SampleEntities.createString(RANDOM, RANDOM.nextInt(10) + 1);
    final String path2 = createPath();
    final Entity id = SampleEntities.createId(RANDOM).toEntity();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), id));
    ex = ex.and(ExpressionFunction.equals(ExpressionFunction.size(ofPath(path2)), NUM_ONE));

    final List<Function> expectedCondition = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.EQUALS)
          .path(ofPath(path))
          .value(id)
          .build(),
        ImmutableFunction.builder()
          .operator(Function.SIZE)
          .path(ofPath(path2))
          .value(NUM_ONE)
          .build());
    equals(expectedCondition, ex);
  }

  // Multiple ExpressionFunctions
  @Test
  void twoEquals() {
    final String path1 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final String path2 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final ConditionExpression ex = ConditionExpression.of(
        ExpressionFunction.equals(ofPath(path1), TRUE_ENTITY),
        ExpressionFunction.equals(ExpressionFunction.size(ofPath(path2)), NUM_ONE));

    final List<Function> expectedCondition = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.EQUALS)
          .path(ofPath(path1))
          .value(TRUE_ENTITY)
          .build(),
        ImmutableFunction.builder()
          .operator(Function.SIZE)
          .path(ofPath(path2))
          .value(NUM_ONE)
          .build());
    equals(expectedCondition, ex);
  }

  @Test
  void threeEquals() {
    final String path1 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final String path2 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final String pathPos = createPathPos();
    final Entity strEntity = SampleEntities.createStringEntity(RANDOM, 10);
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path1), TRUE_ENTITY),
        ExpressionFunction.equals(ofPath(path2), FALSE_ENTITY), ExpressionFunction.equals(ofPath(pathPos), strEntity));

    final List<Function> expectedCondition = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.EQUALS)
          .path(ofPath(path1))
          .value(TRUE_ENTITY)
          .build(),
        ImmutableFunction.builder()
          .operator(Function.EQUALS)
          .path(ofPath(path2))
          .value(FALSE_ENTITY)
          .build(),
        ImmutableFunction.builder()
          .operator(Function.EQUALS)
          .path(ofPath(pathPos))
          .value(strEntity)
          .build());
    equals(expectedCondition, ex);
  }

  // Negative tests
  @Test
  void conditionSizeNotSupported() {
    final String path = createPath();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.size(ofPath(path)));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionAttributeNotExistsNotSupported() {
    final String path = createPath();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.attributeNotExists(ofPath(path)));
    failsUnsupportedOperationException(ex);
  }

  @Test
  void conditionNestedAttributeNotExistsNotSupported() {
    final String path = createPath();
    final ConditionExpression ex =
        ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.attributeNotExists(ofPath(path)), TWO));
    failsUnsupportedOperationException(ex);
  }

  // other tests
  @Test
  void stringEqualsHolder() {
    final String path = createPath();
    final Entity strEntity = SampleEntities.createStringEntity(RANDOM, 7);
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), strEntity));

    final Function expectedFunction = ImmutableFunction.builder()
        .operator(Function.EQUALS)
        .path(ofPath(path))
        .value(strEntity)
        .build();

    Assertions.assertEquals(ImmutableList.of(expectedFunction), RocksDBStore.translate(ex));
  }

  private static void equalsEntity(Entity entity) {
    final String path = createPath();
    equalsEntity(entity, path);
  }

  private static void equalsEntity(Entity entity, String path) {
    final ConditionExpression expression = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), entity));

    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.EQUALS)
          .path(ofPath(path))
          .value(entity)
          .build());
    equals(expectedFunctions, expression);
  }

  /**
   * Create a path from a . delimited string.
   * @param path the input string where parts of the path are . delimited.
   * @return the associated ExpressionPath.
   */
  private static ExpressionPath ofPath(String path) {
    ExpressionPath.PathSegment.Builder builder = null;
    for (String part : path.split("\\.")) {
      if (builder == null) {
        builder = ExpressionPath.builder(part);
      } else {
        try {
          builder = builder.position(Integer.parseInt(part));
        } catch (NumberFormatException e) {
          builder = builder.name(part);
        }
      }
    }

    return builder.build();
  }

  private static String createPath() {
    return SampleEntities.createString(RANDOM, RANDOM.nextInt(15) + 1);
  }

  private static String createPathPos() {
    return SampleEntities.createString(RANDOM, RANDOM.nextInt(8) + 1) + "." + RANDOM.nextInt(10);
  }

  private static void equals(List<Function> expected, ConditionExpression input) {
    Assertions.assertEquals(expected, RocksDBStore.translate(input));
  }

  private static void failsUnsupportedOperationException(ConditionExpression input) {
    assertThrows(UnsupportedOperationException.class, () -> RocksDBStore.translate(input));
  }
}

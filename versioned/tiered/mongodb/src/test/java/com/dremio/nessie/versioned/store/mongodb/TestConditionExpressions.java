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
package com.dremio.nessie.versioned.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Random;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ConditionExpression;
import com.dremio.nessie.versioned.impl.condition.ExpressionFunction;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Store;
import com.google.common.collect.ImmutableMap;

class TestConditionExpressions {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity ONE = Entity.ofString("one");
  private static final Entity TWO = Entity.ofString("two");
  private static final Entity THREE = Entity.ofString("three");

  private static final Entity TRUE_ENTITY = Entity.ofBoolean(true);
  private static final Entity FALSE_ENTITY = Entity.ofBoolean(false);
  private static final Entity LIST_ENTITY = Entity.ofList(ONE, TWO, THREE);
  private static final Entity MAP_ENTITY = Entity.ofMap(ImmutableMap.of("key_one", ONE, "key_two", TWO, "key_three", THREE));

  private static final BsonConditionVisitor BSON_CONDITION_EXPRESSION_VISITOR = new BsonConditionVisitor();

  // Single ExpressionFunction equals tests
  @Test
  void equalsBooleanTrue() {
    final String path = createPath();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), TRUE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": true}]}", path), ex);
  }

  @Test
  void equalsBooleanFalse() {
    final String path = createPath();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), FALSE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": false}]}", path), ex);
  }

  @Test
  void equalsList() {
    final String path = createPath();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), LIST_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": [\"one\", \"two\", \"three\"]}]}", path), ex);
  }

  @Test
  void equalsMap() {
    final String path = createPath();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), MAP_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": {\"key_one\": \"one\", \"key_two\": \"two\", \"key_three\": \"three\"}}]}", path), ex);
  }

  @Test
  void equalsNumber() {
    final String path = createPath();
    final Entity numEntity = Entity.ofNumber(RANDOM.nextLong());
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), numEntity));
    equals(String.format("{\"$and\": [{\"%s\": %d}]}", path, numEntity.getNumber()), ex);
  }

  @Test
  void equalsString() {
    final String path = createPath();
    final Entity strEntity = SampleEntities.createStringEntity(RANDOM, 7);
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), strEntity));
    equals(String.format("{\"$and\": [{\"%s\": \"%s\"}]}", path, strEntity.getString()), ex);
  }

  @Test
  void equalsBinary() {
    final String path = createPath();
    final Entity binaryEntity = Entity.ofBinary(SampleEntities.createBinary(RANDOM, 15));
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), binaryEntity));
    equals(String.format("{\"$and\": [{\"%s\": %s}]}", path, BsonConditionVisitor.toMongoExpression(binaryEntity)), ex);
  }

  // Single ExpressionFunction array equals tests
  @Test
  void arraySubpathEqualsBooleanTrue() {
    final String path = createPathPos();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), TRUE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": true}]}", path), ex);
  }

  @Test
  void arraySubpathEqualsBooleanFalse() {
    final String path = createPathPos();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), FALSE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": false}]}", path), ex);
  }

  @Test
  void arraySubpathEqualsList() {
    final String path = createPathPos();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), LIST_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": [\"one\", \"two\", \"three\"]}]}", path), ex);
  }

  @Test
  void arraySubpathEqualsMap() {
    final String path = createPathPos();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), MAP_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": {\"key_one\": \"one\", \"key_two\": \"two\", \"key_three\": \"three\"}}]}", path), ex);
  }

  @Test
  void arraySubpathEqualsNumber() {
    final String path = createPathPos();
    final Entity numEntity = Entity.ofNumber(RANDOM.nextLong());
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), numEntity));
    equals(String.format("{\"$and\": [{\"%s\": %d}]}", path, numEntity.getNumber()), ex);
  }

  @Test
  void arraySubpathEqualsString() {
    final String path = createPathPos();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), THREE));
    equals(String.format("{\"$and\": [{\"%s\": \"three\"}]}", path), ex);
  }

  @Test
  void arraySubpathWithBinary() {
    final String path = createPathPos();
    final Entity binaryEntity = Entity.ofBinary(SampleEntities.createBinary(RANDOM, 8));
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), binaryEntity));
    equals(String.format("{\"$and\": [{\"%s\": %s}]}", path, BsonConditionVisitor.toMongoExpression(binaryEntity)), ex);
  }

  // Single ExpressionFunction array equals tests
  @Test
  void subpathEqualsBooleanTrue() {
    final String path = createPathName();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), TRUE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": true}]}", path), ex);
  }

  @Test
  void subpathEqualsBooleanFalse() {
    final String path = createPathName();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), FALSE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": false}]}", path), ex);
  }

  @Test
  void subpathEqualsList() {
    final String path = createPathName();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), LIST_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": [\"one\", \"two\", \"three\"]}]}", path), ex);
  }

  @Test
  void subpathEqualsMap() {
    final String path = createPathName();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), MAP_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": {\"key_one\": \"one\", \"key_two\": \"two\", \"key_three\": \"three\"}}]}", path), ex);
  }
  // TODO: map with list

  @Test
  void subpathEqualsNumber() {
    final String path = createPathName();
    final Entity numEntity = Entity.ofNumber(RANDOM.nextLong());
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), numEntity));
    equals(String.format("{\"$and\": [{\"%s\": %d}]}", path, numEntity.getNumber()), ex);
  }

  @Test
  void subpathEqualsString() {
    final String path = createPathName();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), THREE));
    equals(String.format("{\"$and\": [{\"%s\": \"three\"}]}", path), ex);
  }

  @Test
  void subpathEqualsBinary() {
    final String path = createPathName();
    final Entity binaryEntity = Entity.ofBinary(SampleEntities.createBinary(RANDOM, 24));
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), binaryEntity));
    equals(String.format("{\"$and\": [{\"%s\": %s}]}", path, BsonConditionVisitor.toMongoExpression(binaryEntity)), ex);
  }

  // Single ExpressionFunction size tests
  @Test
  void size() {
    final String path = createPath();
    final ConditionExpression ex =
        ConditionExpression.of(ExpressionFunction.equals(ExpressionFunction.size(ofPath(path)), Entity.ofNumber(4)));
    equals(String.format("{\"$and\": [{\"%s\": {\"$size\": 4}}]}", path), ex);
  }

  @Test
  void equalsAndSize() {
    final String path = SampleEntities.createString(RANDOM, RANDOM.nextInt(5) + 1) + "." + RANDOM.nextInt(10) + "."
        + SampleEntities.createString(RANDOM, RANDOM.nextInt(10) + 1);
    final String path2 = createPath();
    final Entity id = SampleEntities.createId(RANDOM).toEntity();
    ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path), id));
    ex = ex.and(ExpressionFunction.equals(ExpressionFunction.size(ofPath(path2)), Entity.ofNumber(1)));
    equals(String.format("{\"$and\": [{\"%s\": %s}, {\"%s\": {\"$size\": 1}}]}", path, BsonConditionVisitor.toMongoExpression(id), path2),
        ex);
  }

  // Multiple ExpressionFunctions
  @Test
  void twoEquals() {
    final String path1 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final String path2 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final ConditionExpression ex = ConditionExpression.of(
        ExpressionFunction.equals(ofPath(path1), TRUE_ENTITY), ExpressionFunction.equals(ofPath(path2), FALSE_ENTITY));
    equals(String.format("{\"$and\": [{\"%s\": true}, {\"%s\": false}]}", path1, path2), ex);
  }

  @Test
  void threeEquals() {
    final String path1 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final String path2 = SampleEntities.createString(RANDOM, RANDOM.nextInt(15));
    final String pathPos = createPathPos();
    final Entity strEntity = SampleEntities.createStringEntity(RANDOM, 10);
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ofPath(path1), TRUE_ENTITY),
        ExpressionFunction.equals(ofPath(path2), FALSE_ENTITY), ExpressionFunction.equals(ofPath(pathPos), strEntity));
    equals(
        String.format("{\"$and\": [{\"%s\": true}, {\"%s\": false}, {\"%s\": \"%s\"}]}", path1, path2, pathPos, strEntity.getString()),
        ex);
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
  void equalsExpression() {
    final ExpressionFunction expressionFunction = ExpressionFunction.equals(ExpressionPath.builder("foo").build(), TRUE_ENTITY);
    Assertions.assertEquals("{\"foo\": true}", expressionFunction.accept(BsonConditionVisitor.VALUE_VISITOR));
  }

  @Test
  void binaryEquals() {
    final Entity id = SampleEntities.createId(RANDOM).toEntity();
    final ConditionExpression ex = ConditionExpression.of(ExpressionFunction.equals(ExpressionPath.builder(Store.KEY_NAME).build(), id));
    equals(String.format("{\"$and\": [{\"id\": %s}]}", BsonConditionVisitor.toMongoExpression(id)), ex);
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

  private static String createPathName() {
    return SampleEntities.createString(RANDOM, RANDOM.nextInt(5) + 1) + "."
        + SampleEntities.createString(RANDOM, RANDOM.nextInt(10) + 1);
  }

  private static void equals(String expected, ConditionExpression input) {
    Assertions.assertEquals(BsonDocument.parse(expected), input.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }

  private static void failsUnsupportedOperationException(ConditionExpression input) {
    assertThrows(UnsupportedOperationException.class, () -> input.accept(BSON_CONDITION_EXPRESSION_VISITOR));
  }
}

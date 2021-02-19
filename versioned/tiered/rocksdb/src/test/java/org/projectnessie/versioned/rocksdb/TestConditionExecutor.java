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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.impl.condition.ExpressionPath;
import org.projectnessie.versioned.store.ConditionFailedException;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.KeyDelta;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestConditionExecutor {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity TRUE_ENTITY = Entity.ofBoolean(true);
  private static final Entity ONE = Entity.ofNumber(1L);
  protected static final Random random = new Random(getRandomSeed());
  private static final String sampleName = createString(random, 10);
  private static final String sampleKey1 = createString(random, 5);
  private static final String sampleKey2 = createString(random, 9);
  private static final int keyListSize = 10;

  protected static long getRandomSeed() {
    return -2938423452345L;
  }

  static final Id ID = createId(new Random(getRandomSeed()));
  static final Id ID_2 = createId(new Random(getRandomSeed()));
  static final Id ID_3 = createId(new Random(getRandomSeed()));
  static final Id ID_4 = createId(new Random(getRandomSeed()));

  @Test
  void executorL1Empty() {
    final String path = createPath();
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(path))
          .value(TRUE_ENTITY)
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1ID() {
    equalsL1(RocksL1.ID, Id.EMPTY.toEntity());
  }

  @Test
  void executorL1CommitMetadata() {
    equalsL1(RocksL1.COMMIT_METADATA, ID.toEntity());
  }

  @Test
  void executorL1IncrementalKeyListCheckpointId() {
    final ExpressionPath expressionPath = ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST).name(RocksL1.CHECKPOINT_ID).build();
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(expressionPath)
          .value(ID.toEntity())
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1IncrementalKeyListDistanceFromCheckpoint() {
    final ExpressionPath expressionPath = ExpressionPath.builder(RocksL1.INCREMENTAL_KEY_LIST)
        .name(RocksL1.DISTANCE_FROM_CHECKPOINT).build();
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(expressionPath)
          .value(ONE)
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1ChildrenSize() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksL1.CHILDREN))
          .value(Entity.ofNumber(RocksL1.SIZE))
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1ChildrenEqualsList() {
    final List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i = 0; i < RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    equalsL1(RocksL1.CHILDREN, Entity.ofList(idsAsEntity));
  }

  @Test
  void executorL1ChildrenEqualsListPosition() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ExpressionPath.builder(RocksL1.CHILDREN).position(3).build())
          .value(ID.toEntity())
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1AncestorsSize() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksL1.ANCESTORS))
          .value(Entity.ofNumber(RocksL1.SIZE))
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1AncestorsEqualsList() {
    final List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i = 0; i < RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    equalsL1(RocksL1.ANCESTORS, Entity.ofList(idsAsEntity));
  }

  @Test
  void executorL1AncestorsEqualsListPosition() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ExpressionPath.builder(RocksL1.ANCESTORS).position(3).build())
          .value(ID.toEntity())
          .build());
    final RocksL1 l1 = createL1(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1FragmentsSize() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksL1.COMPLETE_KEY_LIST))
          .value(Entity.ofNumber(RocksL1.SIZE))
          .build());
    final RocksL1 l1 = createL1CompleteKeyList(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1FragmentsEqualsList() {
    final List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i = 0; i < RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksL1.COMPLETE_KEY_LIST))
          .value(Entity.ofList(idsAsEntity))
          .build());
    final RocksL1 l1 = createL1CompleteKeyList(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL1FragmentsEqualsListPosition() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ExpressionPath.builder(RocksL1.COMPLETE_KEY_LIST).position(3).build())
          .value(ID.toEntity())
          .build());
    final RocksL1 l1 = createL1CompleteKeyList(random);
    Assertions.assertDoesNotThrow(() -> {
      l1.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL2Empty() {
    final String path = createPath();
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(path))
          .value(TRUE_ENTITY)
          .build());
    final RocksL2 l2 = createL2();
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      l2.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL2ID() {
    equalsL2(RocksL2.ID, Id.EMPTY.toEntity());
  }

  @Test
  void executorL2ChildrenSize() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksL1.CHILDREN))
          .value(Entity.ofNumber(RocksL1.SIZE))
          .build());
    final RocksL2 l2 = createL2();
    Assertions.assertDoesNotThrow(() -> {
      l2.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL2ChildrenEqualsList() {
    final List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i = 0; i < RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    equalsL2(RocksL1.CHILDREN, Entity.ofList(idsAsEntity));
  }

  @Test
  void executorL2ChildrenEqualsListPosition() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ExpressionPath.builder(RocksL1.CHILDREN).position(3).build())
          .value(ID.toEntity())
          .build());
    final RocksL2 l2 = createL2();
    Assertions.assertDoesNotThrow(() -> {
      l2.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL3Empty() {
    final String path = createPath();
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(path))
          .value(TRUE_ENTITY)
          .build());
    final RocksL3 l3 = createL3();
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      l3.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorL3ID() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksL3.ID))
          .value(Id.EMPTY.toEntity())
          .build());
    final RocksL3 l3 = createL3();
    Assertions.assertDoesNotThrow(() -> {
      l3.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorCommitMetadataId() {
    final Id id = Id.build("test-id");
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksCommitMetadata.ID))
          .value(id.toEntity())
          .build());
    final RocksCommitMetadata meta = (RocksCommitMetadata) RocksCommitMetadata.of(
        id, 0L, ByteString.EMPTY);
    Assertions.assertDoesNotThrow(() -> {
      meta.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorCommitMetadataValue() {
    final ByteString value = ByteString.copyFrom(createBinary(random, 6));
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksCommitMetadata.VALUE))
          .value(Entity.ofBinary(value))
          .build());
    final RocksCommitMetadata meta = (RocksCommitMetadata) RocksCommitMetadata.of(Id.EMPTY, 0L, value);
    Assertions.assertDoesNotThrow(() -> {
      meta.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorCommitMetadataIdNoMatch() {
    final Id searchId = Id.build("search-id");
    final Id actualId = Id.build("actual-id");
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksCommitMetadata.ID))
          .value(searchId.toEntity())
          .build());
    final RocksCommitMetadata meta = (RocksCommitMetadata) RocksCommitMetadata.of(actualId, 0L, ByteString.EMPTY);
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      meta.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorCommitMetadataValueNoMatch() {
    final ByteString searchValue = ByteString.copyFrom(createBinary(random, 6));
    final ByteString actualValue = ByteString.copyFrom(createBinary(random, 6));
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksCommitMetadata.VALUE))
          .value(Entity.ofBinary(searchValue))
          .build());
    final RocksCommitMetadata meta = (RocksCommitMetadata) RocksCommitMetadata.of(Id.EMPTY, 0L, actualValue);
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      meta.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorTagEmpty() {
    final String path = createPath();
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(path))
          .value(TRUE_ENTITY)
          .build());
    final RocksRef ref = createTag();
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      ref.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorTagID() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(RocksRef.ID))
          .value(Id.EMPTY.toEntity())
          .build());
    final RocksRef ref = createTag();
    Assertions.assertDoesNotThrow(() -> {
      ref.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorTagName() {
    equalsTag(RocksRef.NAME, Entity.ofString(sampleName));
  }

  @Test
  void executorTagCommit() {
    equalsTag(RocksRef.COMMIT, ID_2.toEntity());
  }

  // Children do not exist for Tags
  @Test
  void executorTagChildrenEqualsListPosition() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ExpressionPath.builder(RocksRef.CHILDREN).position(6).build())
          .value(ID.toEntity())
          .build());
    final RocksRef ref = createTag();
    Assertions.assertThrows(ConditionFailedException.class, () -> {
      ref.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorBranchID() {
    equalsBranch(RocksRef.ID, Id.EMPTY.toEntity());
  }

  @Test
  void executorBranchName() {
    equalsBranch(RocksRef.NAME, Entity.ofString(sampleName));
  }

  @Test
  void executorBranchChildrenSize() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksRef.CHILDREN))
          .value(Entity.ofNumber(RocksL1.SIZE))
          .build());
    final RocksRef ref = createBranch(random);
    Assertions.assertDoesNotThrow(() -> {
      ref.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorBranchChildrenEqualsList() {
    final List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i = 0; i < RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    equalsBranch(RocksRef.CHILDREN, Entity.ofList(idsAsEntity));
  }

  @Test
  void executorBranchChildrenEqualsListPosition() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ExpressionPath.builder(RocksRef.CHILDREN).position(8).build())
          .value(ID.toEntity())
          .build());
    final RocksRef ref = createBranch(random);
    Assertions.assertDoesNotThrow(() -> {
      ref.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorBranchMetadata() {
    equalsBranch(RocksRef.METADATA, ID.toEntity());
  }

  @Test
  void executorBranchCommitsSize() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksRef.COMMITS))
          .value(Entity.ofNumber(2))
          .build());
    final RocksRef ref = createBranch(random);
    Assertions.assertDoesNotThrow(() -> {
      ref.evaluate(expectedFunctions);
    });
  }

  @Test
  void executorFragmentID() {
    equalsFragment(RocksFragment.ID, Id.EMPTY.toEntity());
  }

  @Test
  void executorFragmentEqualsKeys() {
    List<Entity> keysList = new ArrayList<>();
    for (int i = 0; i < keyListSize; i++) {
      final Entity key1 = Entity.ofList(Entity.ofString(sampleKey1), Entity.ofString(sampleKey2), Entity.ofString(String.valueOf(i)));
      keysList.add(key1);
    }
    equalsFragment(RocksFragment.KEY_LIST, Entity.ofList(keysList));
  }

  @Test
  void executorFragmentSizeKeys() {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.SIZE)
          .path(ofPath(RocksFragment.KEY_LIST))
          .value(Entity.ofNumber(keyListSize))
          .build());
    final RocksFragment fragment = createFragment();
    Assertions.assertDoesNotThrow(() -> {
      fragment.evaluate(expectedFunctions);
    });
  }

  private static String createPath() {
    return SampleEntities.createString(RANDOM, RANDOM.nextInt(15) + 1);
  }

  private static void equalsL1(String path, Entity entity) {
    equalsRocksBaseValue(path, entity, createL1(random));
  }

  private static void equalsL2(String path, Entity entity) {
    equalsRocksBaseValue(path, entity, createL2());
  }

  private static void equalsFragment(String path, Entity entity) {
    equalsRocksBaseValue(path, entity, createFragment());
  }

  private static void equalsTag(String path, Entity entity) {
    equalsRocksBaseValue(path, entity, createTag());
  }

  private static void equalsBranch(String path, Entity entity) {
    equalsRocksBaseValue(path, entity, createBranch(random));
  }

  private static void equalsRocksBaseValue(String path, Entity entity, RocksBaseValue<?> rocksBaseValue) {
    final List<Function> expectedFunctions = ImmutableList.of(
        ImmutableFunction.builder()
          .operator(Function.Operator.EQUALS)
          .path(ofPath(path))
          .value(entity)
          .build());
    Assertions.assertDoesNotThrow(() -> {
      rocksBaseValue.evaluate(expectedFunctions);
    });
  }

  /**
   * Create a Sample L1 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L1 entity.
   */
  static RocksL1 createL1(Random random) {
    return (RocksL1) new RocksL1()
        .id(Id.EMPTY)
        .commitMetadataId(ID)
        .children(Stream.generate(() -> ID).limit(RocksL1.SIZE))
        .ancestors(Stream.generate(() -> ID).limit(RocksL1.SIZE))
        .keyMutations(Stream.of(Key.of(createString(random, 8), createString(random, 9)).asAddition()))
        .incrementalKeyList(ID, 1);
  }

  /**
   * Create a sample L1 entity with a complete key list, aka Fragment.
   * @param random  object to use for randomization of entity creation.
   * @return sample L1 entity
   */
  static RocksL1 createL1CompleteKeyList(Random random) {
    return (RocksL1) new RocksL1()
        .id(Id.EMPTY)
        .commitMetadataId(ID)
        .children(Stream.generate(() -> ID).limit(RocksL1.SIZE))
        .ancestors(Stream.generate(() -> ID).limit(RocksL1.SIZE))
        .keyMutations(Stream.of(Key.of(createString(random, 8), createString(random, 9)).asAddition()))
        .completeKeyList(Stream.generate(() -> ID).limit(RocksL1.SIZE));
  }

  /**
   * Create a Sample L2 entity.
   * @return sample L2 entity.
   */
  static RocksL2 createL2() {
    return (RocksL2) new RocksL2().id(Id.EMPTY).children(Stream.generate(() -> ID).limit(RocksL1.SIZE));
  }

  /**
   * Create a Sample L2 entity.
   * @return sample L2 entity.
   */
  static RocksL3 createL3() {
    return (RocksL3) new RocksL3()
      .id(Id.EMPTY)
      .keyDelta(Stream.of(KeyDelta.of(
        Key.of(createString(random, 8), createString(random, 9)),
        Id.EMPTY)));
  }

  /**
   * Create a sample Branch (Ref) entity.
   * @param random  object to use for randomization of entity creation.
   * @return sample Ref entity
   */
  static RocksRef createBranch(Random random) {
    return (RocksRef) new RocksRef()
      .id(Id.EMPTY)
      .name(sampleName)
      .branch()
        .children(Stream.generate(() -> ID).limit(RocksL1.SIZE))
        .metadata(ID)
        .commits(bc -> {
          bc.id(ID)
            .commit(ID_2)
            .saved()
              .parent(ID_3)
              .done();
          bc.id(ID_4)
            .commit(ID)
            .unsaved()
              .delta(1, ID_2, ID_3)
              .mutations()
                .keyMutation(Key.of(createString(random, 8), createString(random, 8)).asAddition())
                .done();
        })
      .backToRef();
  }

  /**
   * Create a sample Tag (Ref) entity.
   * @return sample Ref entity
   */
  static RocksRef createTag() {
    return (RocksRef) new RocksRef()
      .id(Id.EMPTY)
      .name(sampleName)
      .tag()
        .commit(ID_2)
        .backToRef();
  }

  /**
   * Create a sample Fragment entity.
   * @return sample Fragment entity
   */
  public static RocksFragment createFragment() {
    return (RocksFragment) new RocksFragment().id(Id.EMPTY)
      .keys(IntStream.range(0, keyListSize)
      .mapToObj(
        i -> Key.of(sampleKey1, sampleKey2, String.valueOf(i))));
  }

  /**
   * Create a Sample ID entity.
   * @param random object to use for randomization of entity creation.
   * @return sample ID entity.
   */
  static Id createId(Random random) {
    return Id.of(createBinary(random, 20));
  }

  /**
   * Create an array of random bytes.
   * @param random random number generator to use.
   * @param numBytes the size of the array.
   * @return the array of random bytes.
   */
  static byte[] createBinary(Random random, int numBytes) {
    final byte[] buffer = new byte[numBytes];
    random.nextBytes(buffer);
    return buffer;
  }

  /**
   * Create a String of random characters.
   * @param random random number generator to use.
   * @param numChars the size of the String.
   * @return the String of random characters.
   */
  static String createString(Random random, int numChars) {
    return random.ints('a', 'z' + 1)
      .limit(numChars)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
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
}

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
package com.dremio.nessie.versioned.store.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.dremio.nessie.tiered.builder.L1;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.impl.SampleEntities;
import com.dremio.nessie.versioned.impl.condition.ExpressionPath;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.Store;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestConditionExecutor {
  private static final Random RANDOM = new Random(8612341233543L);
  private static final Entity TRUE_ENTITY = Entity.ofBoolean(true);
  private static final Entity FALSE_ENTITY = Entity.ofBoolean(false);
  private static final Entity ONE = Entity.ofNumber(1L);
  protected static Random random;
  protected Store store;
  private final String SEPARATOR = ".";

  @BeforeEach
  void setup() {
    random = new Random(getRandomSeed());
  }

  protected static final long getRandomSeed() {
    return -2938423452345L;
  }
  static final Id ID = createId(new Random(getRandomSeed()));

  @Test
  public void executorL1Empty() {
    final Condition condition = new Condition();
    final String path = createPath();
    condition.add(new Function(Function.EQUALS, path, TRUE_ENTITY));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertFalse(l1.evaluate(condition));
  }

  @Test
  public void executorL1CommitMetadata() {
    final Condition condition = new Condition();
    condition.add(new Function(Function.EQUALS, RocksL1.COMMIT_METADATA, ID.toEntity()));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1IncrementalKeyListCheckpointId() {
    final Condition condition = new Condition();
    StringBuilder str = new StringBuilder().append(RocksL1.INCREMENTAL_KEY_LIST).append(SEPARATOR).append(RocksL1.CHECKPOINT_ID);
    condition.add(new Function(Function.EQUALS, str.toString(), ID.toEntity()));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1IncrementalKeyListDistanceFromCheckpoint() {
    final Condition condition = new Condition();
    StringBuilder str = new StringBuilder().append(RocksL1.INCREMENTAL_KEY_LIST).append(SEPARATOR).append(RocksL1.DISTANCE_FROM_CHECKPOINT);
    condition.add(new Function(Function.EQUALS, str.toString(), ONE));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1ChildrenSize() {
    final Condition condition = new Condition();
    condition.add(new Function(Function.SIZE, RocksL1.CHILDREN, Entity.ofNumber(RocksL1.SIZE)));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1ChildrenEqualsList() {
    List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i=0; i<RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    final Condition condition = new Condition();
    condition.add(new Function(Function.EQUALS, RocksL1.CHILDREN, Entity.ofList(idsAsEntity)));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1ChildrenEqualsListPosition() {
    final Condition condition = new Condition();
    StringBuilder str = new StringBuilder().append(RocksL1.CHILDREN).append("(").append("3").append(")");
    condition.add(new Function(Function.EQUALS, str.toString(), ID.toEntity()));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1AncestorsSize() {
    final Condition condition = new Condition();
    condition.add(new Function(Function.SIZE, RocksL1.ANCESTORS, Entity.ofNumber(RocksL1.SIZE)));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1AncestorsEqualsList() {
    List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i=0; i<RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    final Condition condition = new Condition();
    condition.add(new Function(Function.EQUALS, RocksL1.ANCESTORS, Entity.ofList(idsAsEntity)));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1AncestorsEqualsListPosition() {
    final Condition condition = new Condition();
    StringBuilder str = new StringBuilder().append(RocksL1.ANCESTORS).append("(").append("3").append(")");
    condition.add(new Function(Function.EQUALS, str.toString(), ID.toEntity()));
    RocksL1 l1 = (RocksL1) createL1(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1FragmentsSize() {
    final Condition condition = new Condition();
    condition.add(new Function(Function.SIZE, RocksL1.COMPLETE_KEY_LIST, Entity.ofNumber(RocksL1.SIZE)));
    RocksL1 l1 = (RocksL1) createL1CompleteKeyList(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1FragmentsEqualsList() {
    List<Entity> idsAsEntity = new ArrayList<>(RocksL1.SIZE);
    for (int i=0; i<RocksL1.SIZE; i++) {
      idsAsEntity.add(ID.toEntity());
    }
    final Condition condition = new Condition();
    condition.add(new Function(Function.EQUALS, RocksL1.COMPLETE_KEY_LIST, Entity.ofList(idsAsEntity)));
    RocksL1 l1 = (RocksL1) createL1CompleteKeyList(random);
    Assertions.assertTrue(l1.evaluate(condition));
  }

  @Test
  public void executorL1FragmentsEqualsListPosition() {
    final Condition condition = new Condition();
    StringBuilder str = new StringBuilder().append(RocksL1.COMPLETE_KEY_LIST).append("(").append("3").append(")");
    condition.add(new Function(Function.EQUALS, str.toString(), ID.toEntity()));
    RocksL1 l1 = (RocksL1) createL1CompleteKeyList(random);
    Assertions.assertTrue(l1.evaluate(condition));
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

  /**
   * Create a Sample L1 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L1 entity.
   */
  public static L1 createL1(Random random) {
    return new RocksL1().commitMetadataId(ID)
      .children(IntStream.range(0, RocksL1.SIZE).mapToObj(x -> ID))
      .ancestors(IntStream.range(0, RocksL1.SIZE).mapToObj(x -> ID))
      .keyMutations(Stream.of(Key.of(createString(random, 8), createString(random, 9)).asAddition()))
      .incrementalKeyList(ID, 1);
  }

  public static L1 createL1CompleteKeyList(Random random) {
    return new RocksL1().commitMetadataId(ID)
      .children(IntStream.range(0, RocksL1.SIZE).mapToObj(x -> ID))
      .ancestors(IntStream.range(0, RocksL1.SIZE).mapToObj(x -> ID))
      .keyMutations(Stream.of(Key.of(createString(random, 8), createString(random, 9)).asAddition()))
      .completeKeyList(IntStream.range(0, RocksL1.SIZE).mapToObj(x -> ID));
  }


  /**
   * Create a Sample ID entity.
   * @param random object to use for randomization of entity creation.
   * @return sample ID entity.
   */
  public static Id createId(Random random) {
    return Id.of(createBinary(random, 20));
  }

  /**
   * Create an array of random bytes.
   * @param random random number generator to use.
   * @param numBytes the size of the array.
   * @return the array of random bytes.
   */
  public static byte[] createBinary(Random random, int numBytes) {
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
  public static String createString(Random random, int numChars) {
    return random.ints('a', 'z' + 1)
      .limit(numChars)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
  }
}

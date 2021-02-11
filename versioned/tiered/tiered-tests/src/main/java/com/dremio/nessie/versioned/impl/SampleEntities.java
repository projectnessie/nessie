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

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.KeyDelta;
import com.google.protobuf.ByteString;

/**
 * This utility class generates sample objects mapping to each enumerate in
 * {@link com.dremio.nessie.versioned.store.ValueType}.
 * This should be moved to versioned/tests once it will not introduce a circular dependency. Currently this also relies
 * on being in the com.dremio.nessie.versioned.impl package for visibility to create the L1, L3, and InternalBranch
 * objects, and should be moved once possible.
 */
public class SampleEntities {
  /**
   * Create a Sample L1 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L1 entity.
   */
  public static InternalL1 createL1(Random random) {
    return EntityType.L1.buildEntity(producer -> producer.commitMetadataId(createId(random))
        .children(IntStream.range(0, InternalL1.SIZE).mapToObj(x -> createId(random)))
        .ancestors(Stream.of(InternalL1.EMPTY.getId(), Id.EMPTY))
        .keyMutations(Stream.of(Key.of(createString(random, 8), createString(random, 9)).asAddition()))
        .incrementalKeyList(InternalL1.EMPTY.getId(), 1));
  }

  /**
   * Create a Sample L2 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L2 entity.
   */
  public static InternalL2 createL2(Random random) {
    return EntityType.L2.buildEntity(producer -> producer.id(createId(random))
        .children(IntStream.range(0, InternalL2.SIZE).mapToObj(x -> createId(random))));
  }

  /**
   * Create a Sample L3 entity.
   * @param random object to use for randomization of entity creation.
   * @return sample L3 entity.
   */
  public static InternalL3 createL3(Random random) {
    return EntityType.L3.buildEntity(producer -> producer.keyDelta(IntStream.range(0, 100)
        .mapToObj(i -> KeyDelta
            .of(Key.of(createString(random, 5), createString(random, 9), String.valueOf(i)),
                createId(random)))));
  }

  /**
   * Create a Sample Fragment entity.
   * @param random object to use for randomization of entity creation.
   * @return sample Fragment entity.
   */
  public static InternalFragment createFragment(Random random) {
    return EntityType.KEY_FRAGMENT.buildEntity(producer -> producer.keys(IntStream.range(0, 10)
        .mapToObj(
            i -> Key.of(createString(random, 5), createString(random, 9), String.valueOf(i)))));
  }

  /**
   * Create a Sample Branch (InternalRef) entity.
   * @param random object to use for randomization of entity creation.
   * @return sample Branch (InternalRef) entity.
   */
  public static InternalRef createBranch(Random random) {
    final String name = createString(random, 10);

    return EntityType.REF.buildEntity(producer -> producer.id(Id.build(name))
        .name(name)
        .branch()
        .children(IntStream.range(0, InternalL1.SIZE).mapToObj(x -> createId(random)))
        .metadata(createId(random))
        .commits(bc -> {
          bc.id(createId(random))
              .commit(createId(random))
              .saved()
                .parent(createId(random))
                .done();
          bc.id(createId(random))
              .commit(createId(random))
              .unsaved()
                .delta(1, createId(random), createId(random))
                .mutations()
                  .keyMutation(Key.of(createString(random, 8), createString(random, 8)).asAddition())
                  .done();
        }));
  }

  /**
   * Create a Sample Tag (InternalRef) entity.
   * @param random object to use for randomization of entity creation.
   * @return sample Tag (InternalRef) entity.
   */
  public static InternalRef createTag(Random random) {
    return EntityType.REF.buildEntity(producer -> producer.id(createId(random))
        .name("tagName")
        .tag()
        .commit(createId(random)));
  }

  /**
   * Create a Sample CommitMetadata entity.
   * @param random object to use for randomization of entity creation.
   * @return sample CommitMetadata entity.
   */
  public static InternalCommitMetadata createCommitMetadata(Random random) {
    return EntityType.COMMIT_METADATA.buildEntity(producer -> producer.id(createId(random))
        .value(ByteString.copyFrom(createBinary(random, 6))));
  }

  /**
   * Create a Sample Value entity.
   * @param random object to use for randomization of entity creation.
   * @return sample Value entity.
   */
  public static InternalValue createValue(Random random) {
    return EntityType.VALUE.buildEntity(producer -> producer.id(createId(random))
        .value(ByteString.copyFrom(createBinary(random, 6))));
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
   * Create a Sample ID entity.
   * @param random object to use for randomization of entity creation.
   * @return sample ID entity.
   */
  public static Id createId(Random random) {
    return Id.of(createBinary(random, 20));
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

  /**
   * Create a String Entity of random characters.
   * @param random random number generator to use.
   * @param numChars the size of the String.
   * @return the String Entity of random characters.
   */
  public static Entity createStringEntity(Random random, int numChars) {
    return Entity.ofString(createString(random, numChars));
  }
}

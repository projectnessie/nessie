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

import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.projectnessie.RandomSource;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.KeyDelta;

import com.google.protobuf.ByteString;

/**
 * This utility class generates sample objects mapping to each enumerate in
 * {@link org.projectnessie.versioned.store.ValueType}.
 * This should be moved to versioned/tests once it will not introduce a circular dependency. Currently this also relies
 * on being in the org.projectnessie.versioned.impl package for visibility to create the L1, L3, and InternalBranch
 * objects, and should be moved once possible.
 */
public class SampleEntities {
  /**
   * Create a Sample L1 entity.
   * @return sample L1 entity.
   */
  public static InternalL1 createL1() {
    return EntityType.L1.buildEntity(producer -> producer.commitMetadataId(createId())
        .children(IntStream.range(0, InternalL1.SIZE).mapToObj(x -> createId()))
        .ancestors(Stream.of(InternalL1.EMPTY.getId(), Id.EMPTY))
        .keyMutations(Stream.of(Key.of(createString(8), createString(9)).asAddition()))
        .incrementalKeyList(InternalL1.EMPTY.getId(), 1));
  }

  /**
   * Create a Sample L2 entity.
   * @return sample L2 entity.
   */
  public static InternalL2 createL2() {
    return EntityType.L2.buildEntity(producer -> producer.id(createId())
        .children(IntStream.range(0, InternalL2.SIZE).mapToObj(x -> createId())));
  }

  /**
   * Create a Sample L3 entity.
   * @return sample L3 entity.
   */
  public static InternalL3 createL3() {
    return EntityType.L3.buildEntity(producer -> producer.keyDelta(IntStream.range(0, 100)
        .mapToObj(i -> KeyDelta
            .of(Key.of(createString(5), createString(9), String.valueOf(i)),
                createId()))));
  }

  /**
   * Create a Sample Fragment entity.
   * @return sample Fragment entity.
   */
  public static InternalFragment createFragment() {
    return EntityType.KEY_FRAGMENT.buildEntity(producer -> producer.keys(IntStream.range(0, 10)
        .mapToObj(
            i -> Key.of(createString(5), createString(9), String.valueOf(i)))));
  }

  /**
   * Create a Sample Branch (InternalRef) entity.
   * @return sample Branch (InternalRef) entity.
   */
  public static InternalRef createBranch() {
    final String name = createString(10);

    return EntityType.REF.buildEntity(producer -> producer.id(Id.build(name))
        .name(name)
        .branch()
        .children(IntStream.range(0, InternalL1.SIZE).mapToObj(x -> createId()))
        .metadata(createId())
        .commits(bc -> {
          bc.id(createId())
              .commit(createId())
              .saved()
              .parent(createId())
              .done();
          bc.id(createId())
              .commit(createId())
              .unsaved()
              .delta(1, createId(), createId())
              .mutations()
              .keyMutation(Key.of(createString(8), createString(8)).asAddition())
              .done();
        }));
  }

  /**
   * Create a Sample Tag (InternalRef) entity.
   * @return sample Tag (InternalRef) entity.
   */
  public static InternalRef createTag() {
    return EntityType.REF.buildEntity(producer -> producer.id(createId())
        .name("tagName")
        .tag()
        .commit(createId()));
  }

  /**
   * Create a Sample CommitMetadata entity.
   * @return sample CommitMetadata entity.
   */
  public static InternalCommitMetadata createCommitMetadata() {
    return EntityType.COMMIT_METADATA.buildEntity(producer -> producer.id(createId())
        .value(ByteString.copyFrom(createBinary(6))));
  }

  /**
   * Create a Sample Value entity.
   * @return sample Value entity.
   */
  public static InternalValue createValue() {
    return EntityType.VALUE.buildEntity(producer -> producer.id(createId())
        .value(ByteString.copyFrom(createBinary(6))));
  }

  /**
   * Create an array of random bytes.
   * @param numBytes the size of the array.
   * @return the array of random bytes.
   */
  public static byte[] createBinary(int numBytes) {
    final byte[] buffer = new byte[numBytes];
    RandomSource.current().nextBytes(buffer);
    return buffer;
  }

  /**
   * Create a Sample ID entity.
   * @return sample ID entity.
   */
  public static Id createId() {
    return Id.of(createBinary(20));
  }

  /**
   * Create a String Entity of random characters.
   * @param numChars the size of the String.
   * @return the String Entity of random characters.
   */
  public static Entity createStringEntity(int numChars) {
    return Entity.ofString(createString(numChars));
  }

  /**
   * Create a String of random characters.
   * @param numChars the size of the String.
   * @return the String of random characters.
   */
  private static String createString(int numChars) {
    return RandomSource.current().ints('a', 'z' + 1)
        .limit(numChars)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }
}

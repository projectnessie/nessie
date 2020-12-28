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

import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * This utility class generates sample objects mapping to each enumerate in {@link
 * com.dremio.nessie.versioned.store.ValueType}. This should be moved to versioned/tests once it
 * will not introduce a circular dependency. Currently this also relies on being in the
 * com.dremio.nessie.versioned.impl package for visibility to create the L1, L3, and InternalBranch
 * objects, and should be moved once possible.
 */
public class SampleEntities {
  /**
   * Create a Sample L1 entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample L1 entity.
   */
  public static L1 createL1(Random random) {
    final List<KeyMutation> mutations =
        ImmutableList.of(
            KeyMutation.KeyAddition.of(
                new InternalKey(Key.of("a", createString(random, 8), createString(random, 9)))));

    final List<Entity> deltaIds = new ArrayList<>(L1.SIZE);
    for (int i = 0; i < L1.SIZE; i++) {
      deltaIds.add(createIdEntity(random));
    }

    return L1.EMPTY.getChildWithTree(
        createId(random),
        IdMap.fromEntity(Entity.ofList(deltaIds), L1.SIZE),
        KeyMutationList.of(mutations));
  }

  /**
   * Create a Sample L2 entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample L2 entity.
   */
  public static L2 createL2(Random random) {
    final List<Entity> treeList = new ArrayList<>(L2.SIZE);
    for (int i = 0; i < L2.SIZE; i++) {
      treeList.add(createIdEntity(random));
    }

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", createIdEntity(random));
    attributeMap.put("tree", Entity.ofList(treeList));

    return L2.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample L3 entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample L3 entity.
   */
  public static L3 createL3(Random random) {
    final int size = 100;
    L3 l3 = L3.EMPTY;
    for (int i = 0; i < size; ++i) {
      l3 =
          l3.set(
              new InternalKey(
                  Key.of(createString(random, 5), createString(random, 9), String.valueOf(i))),
              createId(random));
    }

    return l3;
  }

  /**
   * Create a Sample Fragment entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample Fragment entity.
   */
  public static Fragment createFragment(Random random) {
    final int size = 10;
    final List<Entity> keyList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      keyList.add(
          Entity.ofList(
              createStringEntity(random, 5),
              createStringEntity(random, 8),
              Entity.ofString(String.valueOf(i))));
    }

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", createIdEntity(random));
    attributeMap.put("keys", Entity.ofList(keyList));

    return Fragment.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Branch (InternalRef) entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample Branch (InternalRef) entity.
   */
  public static InternalRef createBranch(Random random) {
    final List<Entity> treeList = new ArrayList<>(L1.SIZE);
    for (int i = 0; i < L1.SIZE; i++) {
      treeList.add(createIdEntity(random));
    }

    // Two commits, one with a DELTA and one without.
    final List<Entity> commitList = new ArrayList<>(2);
    commitList.add(
        Entity.ofMap(
            ImmutableMap.of(
                "id", createIdEntity(random),
                "commit", createIdEntity(random),
                "parent", createIdEntity(random))));
    commitList.add(
        Entity.ofMap(
            ImmutableMap.of(
                "id", createIdEntity(random),
                "commit", createIdEntity(random),
                "deltas",
                    Entity.ofList(
                        Entity.ofMap(
                            ImmutableMap.of(
                                "position", Entity.ofNumber(1),
                                "old", createIdEntity(random),
                                "new", createIdEntity(random)))),
                "keys",
                    Entity.ofList(
                        Entity.ofMap(
                            ImmutableMap.of(
                                "a",
                                Entity.ofList(
                                    createStringEntity(random, 8),
                                    createStringEntity(random, 8))))))));

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put(InternalRef.TYPE, Entity.ofString("b"));
    final String name = createString(random, 10);
    attributeMap.put("name", Entity.ofString(name));
    attributeMap.put("id", Id.build(name).toEntity());
    attributeMap.put("tree", Entity.ofList(treeList));
    attributeMap.put("metadata", createIdEntity(random));
    attributeMap.put("commits", Entity.ofList(commitList));

    return InternalRef.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Tag (InternalRef) entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample Tag (InternalRef) entity.
   */
  public static InternalRef createTag(Random random) {
    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put(InternalRef.TYPE, Entity.ofString("t"));
    attributeMap.put("id", createIdEntity(random));
    attributeMap.put("name", Entity.ofString("tagName"));
    attributeMap.put("commit", createIdEntity(random));

    return InternalRef.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample CommitMetadata entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample CommitMetadata entity.
   */
  public static InternalCommitMetadata createCommitMetadata(Random random) {
    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", createIdEntity(random));
    attributeMap.put("value", Entity.ofBinary(createBinary(random, 6)));

    return InternalCommitMetadata.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Value entity.
   *
   * @param random object to use for randomization of entity creation.
   * @return sample Value entity.
   */
  public static InternalValue createValue(Random random) {
    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", createIdEntity(random));
    attributeMap.put("value", Entity.ofBinary(createBinary(random, 7)));

    return InternalValue.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create an array of random bytes.
   *
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
   *
   * @param random random number generator to use.
   * @param numChars the size of the String.
   * @return the String of random characters.
   */
  private static String createString(Random random, int numChars) {
    return random
        .ints('a', 'z' + 1)
        .limit(numChars)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  private static Id createId(Random random) {
    return Id.of(createBinary(random, 20));
  }

  private static Entity createIdEntity(Random random) {
    return Entity.ofBinary(createBinary(random, 20));
  }

  private static Entity createStringEntity(Random random, int numChars) {
    return Entity.ofString(createString(random, numChars));
  }
}

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

/**
 * This utility class generates sample objects mapping to each enumerate in
 * {@link com.dremio.nessie.versioned.store.ValueType}.
 * It is intended to be a central place for test data generation used by multiple test suites.
 */
public class SampleEntities {
  private static final Entity ID = Entity.ofBinary(
      new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  private static final Entity ID2 = Entity.ofBinary(
      new byte[] {19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0});

  /**
   * Create a Sample L1 entity.
   * @return sample L1 entity.
   */
  public static L1 createL1() {
    final Map<String, Entity> keyList = new HashMap<>();
    keyList.put("chk", Entity.ofBoolean(true));
    keyList.put("fragments", Entity.ofList(ID, ID2, ID));
    keyList.put("mutations", Entity.ofList(Entity.ofMap(ImmutableMap.of("a",
        Entity.ofList(Entity.ofString("addition1"), Entity.ofString("addition2"))))));

    final List<Entity> treeList = new ArrayList<>(L1.SIZE);
    for (int i = 0; i < L1.SIZE; i++) {
      treeList.add(ID);
    }

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("metadata", ID);
    attributeMap.put("tree", Entity.ofList(treeList));
    // The ID is generated from the contents of the L1 object. If any of the contents change, the ID will as well.
    attributeMap.put("id", Entity.ofBinary(
        new byte[] {110, 40, 7, 86, -103, 23, 6, -34, -102, 95, -33, 77, -87, 31, 23, 110, 67, -46, 56, -45}));
    attributeMap.put("keys", Entity.ofMap(keyList));
    attributeMap.put("parents", Entity.ofList(ID2));

    return L1.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample L2 entity.
   * @return sample L2 entity.
   */
  public static L2 createL2() {
    final List<Entity> treeList = new ArrayList<>(L2.SIZE);
    for (int i = 0; i < L2.SIZE; i++) {
      treeList.add(ID);
    }

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", ID);
    attributeMap.put("tree", Entity.ofList(treeList));

    return L2.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample L3 entity.
   * @return sample L3 entity.
   */
  public static L3 createL3() {
    final int size = 100;
    final List<Entity> treeList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      treeList.add(Entity.ofMap(ImmutableMap.of(
          "key", Entity.ofList(Entity.ofString("path1"), Entity.ofString("path2"), Entity.ofString(String.valueOf(i))),
          "id", ID2
      )));
    }

    final Map<String, Entity> attributeMap = new HashMap<>();
    // The ID is generated from the contents of the L3 object. If any of the contents change, the ID will as well.
    attributeMap.put("id", Entity.ofBinary(
        new byte[] {-42, 83, 119, 97, -12, 87, -124, 124, 7, -50, 86, 39, -101, -99, -90, 40, 62, -127, 93, 13}));
    attributeMap.put("tree", Entity.ofList(treeList));

    return L3.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Fragment entity.
   * @return sample Fragment entity.
   */
  public static Fragment createFragment() {
    final int size = 10;
    final List<Entity> keyList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      keyList.add(Entity.ofList(Entity.ofString("path1"), Entity.ofString("path2"), Entity.ofString(String.valueOf(i))));
    }

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", ID);
    attributeMap.put("keys", Entity.ofList(keyList));

    return Fragment.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Branch (InternalRef) entity.
   * @return sample Branch (InternalRef) entity.
   */
  public static InternalRef createBranch() {
    final List<Entity> treeList = new ArrayList<>(L1.SIZE);
    for (int i = 0; i < L1.SIZE; i++) {
      treeList.add(ID);
    }

    // Two commits, one with a DELTA and one without.
    final List<Entity> commitList = new ArrayList<>(2);
    commitList.add(Entity.ofMap(ImmutableMap.of(
        "id", ID,
        "commit", ID2,
        "parent", ID2
    )));
    commitList.add(Entity.ofMap(ImmutableMap.of(
        "id", ID,
        "commit", ID2,
        "deltas", Entity.ofList(Entity.ofMap(ImmutableMap.of(
            "position", Entity.ofNumber(1),
            "old", ID,
            "new", ID2
        ))),
        "keys", Entity.ofList(Entity.ofMap(ImmutableMap.of("a",
            Entity.ofList(Entity.ofString("addition1"), Entity.ofString("addition2")))))
    )));

    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put(InternalRef.TYPE, Entity.ofString("b"));
    attributeMap.put("id", Entity.ofBinary(
        new byte[] {-53, 113, -70, -14, -89, 123, -56, 32, -83, -44, -7, 10, 10, -94, -88, 85, 85, 52, -19, 80}));
    attributeMap.put("name", Entity.ofString("branchName"));
    attributeMap.put("tree", Entity.ofList(treeList));
    attributeMap.put("metadata", ID2);
    attributeMap.put("commits", Entity.ofList(commitList));

    return InternalRef.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Tag (InternalRef) entity.
   * @return sample Tag (InternalRef) entity.
   */
  public static InternalRef createTag() {
    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put(InternalRef.TYPE, Entity.ofString("t"));
    attributeMap.put("id", ID);
    attributeMap.put("name", Entity.ofString("tagName"));
    attributeMap.put("commit", ID2);

    return InternalRef.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample CommitMetadata entity.
   * @return sample CommitMetadata entity.
   */
  public static InternalCommitMetadata createCommitMetadata() {
    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", ID);
    attributeMap.put("value", Entity.ofBinary(new byte[]{1, 5, 10, 15, 25, 50}));

    return InternalCommitMetadata.SCHEMA.mapToItem(attributeMap);
  }

  /**
   * Create a Sample Value entity.
   * @return sample Value entity.
   */
  public static InternalValue createValue() {
    final Map<String, Entity> attributeMap = new HashMap<>();
    attributeMap.put("id", ID2);
    attributeMap.put("value", Entity.ofBinary(new byte[]{-50, -24, 0, 4, 99, -100, 111}));

    return InternalValue.SCHEMA.mapToItem(attributeMap);
  }
}

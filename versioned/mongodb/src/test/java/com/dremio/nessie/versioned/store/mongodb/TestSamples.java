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
public class TestSamples {
  private static final Entity ID = Entity.ofBinary(
    new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  private static final Entity ID2 = Entity.ofBinary(
    new byte[] {19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0});

  /**
   * Create a Sample L1 schema.
   * @return sample L1 schema.
   */
  public static L1 getSampleL1() {
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
   * Create a Sample L2 schema.
   * @return sample L2 schema.
   */
  public static L2 getSampleL2() {
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
   * Create a Sample L3 schema.
   * @return sample L3 schema.
   */
  public static L3 getSampleL3() {
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
}

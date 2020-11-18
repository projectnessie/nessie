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

import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.store.Entity;

/**
 * This utility class generates sample objects mapping to each enumerate in {@link com.dremio.nessie.versioned.store.ValueType}.
 * It is intended to be a central place for test data generation used by multiple test suites.
 */
public class TestValueTypeUtility {
  public static L2 getSampleL2() {
    Map<String, Entity> attributeMap = new HashMap<>();
    byte[] idBytes = new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19};
    List<Entity> treeList = new ArrayList<Entity>(199);
    for (int i=0; i<199; i++) {
      treeList.add(Entity.ofBinary(idBytes));
    }
    attributeMap.put("id", Entity.ofBinary(idBytes));
    attributeMap.put("tree", Entity.ofList(treeList));
    L2 l2 = L2.SCHEMA.mapToItem(attributeMap);
    return l2;
  }
}

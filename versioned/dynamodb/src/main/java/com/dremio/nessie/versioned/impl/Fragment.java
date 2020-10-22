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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class Fragment extends MemoizedId {

  private final List<InternalKey> keys;

  public Fragment(List<InternalKey> keys) {
    super();
    this.keys = ImmutableList.copyOf(keys);
  }

  @Override
  Id generateId() {
    return Id.build(h -> {
      keys.stream().forEach(k -> InternalKey.addToHasher(k, h));
    });
  }

  public List<InternalKey> getKeys() {
    return keys;
  }

  public static final SimpleSchema<Fragment> SCHEMA = new SimpleSchema<Fragment>(Fragment.class) {
    private static final String ID = "id";
    private static final String KEYS = "keys";

    @Override
    public Fragment deserialize(Map<String, Entity> attributeMap) {
      List<InternalKey> keys = attributeMap.get(KEYS).l().stream().map(InternalKey::fromEntity).collect(Collectors.toList());
      return new Fragment(keys);
    }

    @Override
    public Map<String, Entity> itemToMap(Fragment item, boolean ignoreNulls) {
      return ImmutableMap.<String, Entity>builder()
          .put(ID, item.getId().toEntity())
          .put(KEYS, Entity.l(item.getKeys().stream().map(InternalKey::toEntity).collect(ImmutableList.toImmutableList())))
          .build();
    }

  };

}

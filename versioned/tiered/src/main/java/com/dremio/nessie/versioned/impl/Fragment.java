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

import static com.dremio.nessie.versioned.impl.ValidationHelper.checkCalled;
import static com.dremio.nessie.versioned.impl.ValidationHelper.checkSet;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dremio.nessie.tiered.builder.FragmentConsumer;
import com.dremio.nessie.tiered.builder.Producer;
import com.dremio.nessie.versioned.Key;
import com.dremio.nessie.versioned.store.Entity;
import com.dremio.nessie.versioned.store.Id;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class Fragment extends MemoizedId<FragmentConsumer> {

  private final List<InternalKey> keys;

  public Fragment(List<InternalKey> keys) {
    super();
    this.keys = ImmutableList.copyOf(keys);
  }

  public Fragment(Id id, List<InternalKey> keys) {
    super(id);
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
      List<InternalKey> keys = attributeMap.get(KEYS).getList().stream().map(InternalKey::fromEntity).collect(Collectors.toList());
      return new Fragment(keys);
    }

    @Override
    public Map<String, Entity> itemToMap(Fragment item, boolean ignoreNulls) {
      return ImmutableMap.<String, Entity>builder()
          .put(ID, item.getId().toEntity())
          .put(KEYS, Entity.ofList(item.getKeys().stream().map(InternalKey::toEntity).collect(ImmutableList.toImmutableList())))
          .build();
    }

  };

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Fragment fragment = (Fragment) o;
    return Objects.equal(keys, fragment.keys);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(keys);
  }

  @Override
  public FragmentConsumer applyToConsumer(FragmentConsumer consumer) {
    consumer.id(getId());
    consumer.keys(keys.stream().map(InternalKey::toKey));
    return consumer;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder implements FragmentConsumer, Producer<Fragment, FragmentConsumer> {

    private Id id;
    private List<InternalKey> keys;

    @Override
    public Builder id(Id id) {
      checkCalled(this.id, "id");
      this.id = id;
      return this;
    }

    @Override
    public boolean canHandleType(ValueType valueType) {
      return valueType == ValueType.KEY_FRAGMENT;
    }

    @Override
    public Builder keys(Stream<Key> keys) {
      checkCalled(this.keys, "keys");
      this.keys = keys.map(InternalKey::new).collect(Collectors.toList());
      return this;
    }

    /**
     * TODO javadoc.
     */
    public Fragment build() {
      checkSet(id, "keys");
      checkSet(keys, "id");

      return new Fragment(id, keys);
    }
  }
}

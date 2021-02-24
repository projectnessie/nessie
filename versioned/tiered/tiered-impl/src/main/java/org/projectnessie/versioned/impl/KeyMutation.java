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

import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.Entity;

import com.google.common.collect.ImmutableMap;

abstract class KeyMutation {

  enum MutationType {
    ADDITION("a"),
    REMOVAL("d");

    private final String field;

    MutationType(String field) {
      this.field = field;
    }
  }

  abstract InternalKey getKey();

  abstract MutationType getType();

  static KeyMutation fromMutation(Key.Mutation km) {
    switch (km.getType()) {
      case ADDITION:
        return KeyMutation.KeyAddition.of(new InternalKey(km.getKey()), ((Key.Addition) km).getEntityType());
      case REMOVAL:
        return KeyMutation.KeyRemoval.of(new InternalKey(km.getKey()));
      default:
        throw new IllegalArgumentException("Unknown mutation-type " + km.getType());
    }
  }

  abstract Key.Mutation toMutation();

  abstract Entity toEntity();

  @Immutable
  public abstract static class KeyAddition extends KeyMutation {

    @Override
    public final MutationType getType() {
      return MutationType.ADDITION;
    }

    public abstract int getEntityType();

    public static KeyAddition of(InternalKey key, int entityType) {
      return ImmutableKeyAddition.builder().key(key).entityType(entityType).build();
    }

    @Override
    Key.Mutation toMutation() {
      return getKey().toKey().asAddition(getEntityType());
    }

    @Override
    Entity toEntity() {
      return Entity.ofMap(ImmutableMap.of(getType().field, getKey().toEntity(), "et", Entity.ofNumber(getEntityType())));
    }
  }

  @Immutable
  public abstract static class KeyRemoval extends KeyMutation {

    @Override
    public final MutationType getType() {
      return MutationType.REMOVAL;
    }

    public static KeyRemoval of(InternalKey key) {
      return ImmutableKeyRemoval.builder().key(key).build();
    }

    @Override
    Key.Mutation toMutation() {
      return getKey().toKey().asRemoval();
    }

    @Override
    Entity toEntity() {
      return Entity.ofMap(ImmutableMap.of(getType().field, getKey().toEntity()));
    }
  }
}

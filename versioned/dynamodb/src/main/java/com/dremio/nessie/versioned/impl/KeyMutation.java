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

import java.util.Map;

import org.immutables.value.Value.Immutable;

import com.dremio.nessie.versioned.store.Entity;
import com.google.common.collect.ImmutableMap;

abstract class KeyMutation {

  static enum MutationType {
    ADDITION("a"),
    REMOVAL("d");

    private final String field;

    MutationType(String field) {
      this.field = field;
    }
  }

  abstract InternalKey getKey();

  abstract MutationType getType();

  @Immutable
  public abstract static class KeyAddition extends KeyMutation {

    @Override
    public final MutationType getType() {
      return MutationType.ADDITION;
    }

    public static KeyAddition of(InternalKey key) {
      return ImmutableKeyAddition.builder().key(key).build();
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

  }

  Entity toEntity() {
    return Entity.m(ImmutableMap.<String, Entity>of(getType().field, getKey().toEntity()));
  }

  public static KeyMutation fromEntity(Entity value) {
    Map<String, Entity> mp = value.m();
    if (mp.containsKey(MutationType.ADDITION.field)) {
      return KeyAddition.of(InternalKey.fromEntity(mp.get(MutationType.ADDITION.field)));
    } else if (mp.containsKey(MutationType.REMOVAL.field)) {
      return KeyRemoval.of(InternalKey.fromEntity(mp.get(MutationType.REMOVAL.field)));
    } else {
      throw new UnsupportedOperationException();
    }
  }
}

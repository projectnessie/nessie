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

import com.google.common.collect.ImmutableMap;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.immutables.value.Value.Immutable;
import org.projectnessie.versioned.store.Entity;
import org.projectnessie.versioned.tiered.Mutation;

abstract class InternalMutation {
  private static final char ZERO_BYTE = '\u0000';

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

  static InternalMutation fromMutation(Mutation km) {
    switch (km.getType()) {
      case ADDITION:
        Mutation.Addition a = (Mutation.Addition) km;
        return InternalAddition.of(new InternalKey(a.getKey()), a.getPayload());
      case REMOVAL:
        return InternalRemoval.of(new InternalKey(km.getKey()));
      default:
        throw new IllegalArgumentException("Unknown mutation-type " + km.getType());
    }
  }

  abstract Mutation toMutation();

  abstract Entity toEntity();

  @Immutable
  public abstract static class InternalAddition extends InternalMutation {

    @Override
    public final MutationType getType() {
      return MutationType.ADDITION;
    }

    public static InternalAddition of(InternalKey key, Byte payload) {
      return ImmutableInternalAddition.builder().key(key).payload(payload).build();
    }

    @Nullable
    public abstract Byte getPayload();

    @Override
    Mutation toMutation() {
      return Mutation.Addition.of(getKey().toKey(), getPayload());
    }

    public Entity toEntity() {
      Entity key = getKey().toEntity();
      Entity payload =
          Entity.ofString(
              getPayload() == null ? Character.toString(ZERO_BYTE) : getPayload().toString());
      return Entity.ofMap(
          ImmutableMap.of(
              getType().field,
              Entity.ofList(Stream.concat(Stream.of(payload), key.getList().stream()))));
    }
  }

  @Immutable
  public abstract static class InternalRemoval extends InternalMutation {

    @Override
    public final MutationType getType() {
      return MutationType.REMOVAL;
    }

    public static InternalRemoval of(InternalKey key) {
      return ImmutableInternalRemoval.builder().key(key).build();
    }

    @Override
    Mutation toMutation() {
      return Mutation.Removal.of(getKey().toKey());
    }

    public Entity toEntity() {
      Entity key = getKey().toEntity();
      Entity payload = Entity.ofString(Character.toString(ZERO_BYTE));
      return Entity.ofMap(
          ImmutableMap.of(
              getType().field,
              Entity.ofList(Stream.concat(Stream.of(payload), key.getList().stream()))));
    }
  }
}

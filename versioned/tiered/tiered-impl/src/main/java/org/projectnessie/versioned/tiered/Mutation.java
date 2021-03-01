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
package org.projectnessie.versioned.tiered;

import javax.annotation.Nullable;

import org.immutables.value.Value;
import org.projectnessie.versioned.Key;

/**
 * Record of a mutation.
 *
 * <p>Can be an addition (eg new key/value) a removal (eg remove k/v) or a modification (change to the payload of the value).
 */
public interface Mutation {
  enum MutationType {
    ADDITION,
    MODIFICATION,
    REMOVAL
  }

  MutationType getType();

  Key getKey();

  @Value.Immutable
  abstract class Addition implements Mutation {

    @Override
    public final MutationType getType() {
      return MutationType.ADDITION;
    }

    @Override
    public abstract Key getKey();

    @Nullable
    public abstract Byte getPayload();

    public static Addition of(Key key, Byte payload) {
      return ImmutableAddition.builder().key(key).payload(payload).build();
    }
  }

  @Value.Immutable
  abstract class Modification implements Mutation {

    @Override
    public final MutationType getType() {
      return MutationType.MODIFICATION;
    }

    @Override
    public abstract Key getKey();

    @Nullable
    public abstract Byte getPayload();

    public static Modification of(Key key, Byte payload) {
      return ImmutableModification.builder().key(key).payload(payload).build();
    }
  }

  @Value.Immutable
  abstract class Removal implements Mutation {

    @Override
    public final MutationType getType() {
      return MutationType.REMOVAL;
    }

    @Override
    public abstract Key getKey();

    public static Removal of(Key key) {
      return ImmutableRemoval.builder().key(key).build();
    }
  }
}

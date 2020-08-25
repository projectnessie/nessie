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

import org.immutables.value.Value;

import com.dremio.nessie.versioned.impl.InternalBranch.UnsavedDelta;

/**
 * Describes the current and previous state of the value.
 */
@Value.Immutable
abstract class PositionDelta {

  static PositionDelta EMPTY_ZERO = PositionDelta.builder().newId(Id.EMPTY).oldId(Id.EMPTY).position(0).build();

  abstract int getPosition();

  @Value.Auxiliary
  abstract Id getOldId();

  abstract Id getNewId();

  static ImmutablePositionDelta.Builder builder() {
    return ImmutablePositionDelta.builder();
  }

  boolean isDirty() {
    return !getOldId().equals(getNewId());
  }

  final boolean isEmpty() {
    return !isDirty() && getOldId().equals(Id.EMPTY);
  }

  static PositionDelta of(int position, Id id) {
    return ImmutablePositionDelta.builder().oldId(id).newId(id).position(position).build();
  }

  final UnsavedDelta toUnsavedDelta() {
    UnsavedDelta delta = new UnsavedDelta(getPosition(), getOldId(), getNewId());
    return delta;
  }


}

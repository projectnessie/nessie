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

import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.store.Id;

/** Describes the current and previous state of the value. */
@Value.Immutable
@Value.Style(builder = "getBuilder")
abstract class PositionDeltaWithPayload extends PositionDelta {

  static final PositionDeltaWithPayload SINGLE_ZERO =
      PositionDeltaWithPayload.builderWithPayload()
          .newId(Id.EMPTY)
          .oldId(Id.EMPTY)
          .position(0)
          .build();

  @Value.Auxiliary
  @Nullable
  abstract Byte getOldPayload();

  @Nullable
  abstract Byte getNewPayload();

  boolean isPayloadDirty() {
    if (getOldPayload() == null) {
      return getNewPayload() != null;
    }
    return !getOldPayload().equals(getNewPayload());
  }

  static ImmutablePositionDeltaWithPayload.Builder builderWithPayload() {
    return ImmutablePositionDeltaWithPayload.getBuilder();
  }

  static PositionDeltaWithPayload of(int position, Id id, Byte payload) {
    return ImmutablePositionDeltaWithPayload.getBuilder()
        .oldId(id)
        .newId(id)
        .position(position)
        .oldPayload(payload)
        .newPayload(payload)
        .build();
  }
}

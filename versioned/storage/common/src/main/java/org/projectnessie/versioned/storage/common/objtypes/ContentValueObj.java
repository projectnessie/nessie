/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.objtypes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.versioned.storage.common.objtypes.Hashes.contentValueHash;

import com.google.protobuf.ByteString;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface ContentValueObj extends Obj {

  @Override
  default ObjType type() {
    return ObjType.VALUE;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  @jakarta.annotation.Nullable
  ObjId id();

  @Value.Parameter(order = 2)
  @Nullable
  @jakarta.annotation.Nullable
  String contentId();

  @Value.Parameter(order = 3)
  int payload();

  @Value.Parameter(order = 4)
  ByteString data();

  @Nonnull
  @jakarta.annotation.Nonnull
  static ContentValueObj contentValue(
      @Nullable @jakarta.annotation.Nullable ObjId id,
      @Nullable @jakarta.annotation.Nullable String contentId,
      int payload,
      @Nonnull @jakarta.annotation.Nonnull ByteString data) {
    checkArgument(payload >= 0 && payload <= 127);
    return ImmutableContentValueObj.of(id, contentId, payload, data);
  }

  @Nonnull
  @jakarta.annotation.Nonnull
  static ContentValueObj contentValue(
      @Nullable @jakarta.annotation.Nullable String contentId,
      int payload,
      @Nonnull @jakarta.annotation.Nonnull ByteString data) {
    return contentValue(contentValueHash(contentId, payload, data), contentId, payload, data);
  }
}

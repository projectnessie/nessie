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

import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.TAG;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjIdHasher;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface TagObj extends Obj {

  @Override
  default ObjType type() {
    return StandardObjType.TAG;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  ObjId id();

  @Override
  @Value.Parameter(order = 1)
  @Value.Auxiliary
  long referenced();

  /** The tag message as plain text. */
  @Value.Parameter(order = 2)
  @Nullable
  String message();

  /** All headers and values. Headers are multi-valued. */
  @Value.Parameter(order = 3)
  @Nullable
  CommitHeaders headers();

  @Value.Parameter(order = 4)
  @Nullable
  ByteString signature();

  static TagObj tag(
      ObjId id, long referenced, String message, CommitHeaders headers, ByteString signature) {
    return ImmutableTagObj.of(id, referenced, message, headers, signature);
  }

  static TagObj tag(String message, CommitHeaders headers, ByteString signature) {
    ObjIdHasher hasher = objIdHasher(TAG);
    if (message != null) {
      hasher.hash(message);
    }
    if (headers != null) {
      hasher.hash(headers);
    }
    if (signature != null) {
      hasher.hash(signature.asReadOnlyByteBuffer());
    }

    return tag(hasher.generate(), 0L, message, headers, signature);
  }
}

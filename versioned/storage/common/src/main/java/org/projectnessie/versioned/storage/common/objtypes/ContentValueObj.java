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
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.VALUE;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
public interface ContentValueObj extends Obj {

  @Override
  default ObjType type() {
    return VALUE;
  }

  @Override
  @Value.Parameter(order = 1)
  ObjId id();

  @Override
  @Value.Parameter(order = 1)
  @Value.Auxiliary
  long referenced();

  @Value.Parameter(order = 2)
  String contentId();

  @Value.Parameter(order = 3)
  int payload();

  @Value.Parameter(order = 4)
  ByteString data();

  static ContentValueObj contentValue(
      ObjId id, long referenced, String contentId, int payload, ByteString data) {
    checkArgument(payload >= 0 && payload <= 127);
    return ImmutableContentValueObj.of(id, referenced, contentId, payload, data);
  }

  static ContentValueObj contentValue(String contentId, int payload, ByteString data) {
    return contentValue(
        objIdHasher(VALUE)
            .hash(contentId)
            .hash(payload)
            .hash(data.asReadOnlyByteBuffer())
            .generate(),
        0L,
        contentId,
        payload,
        data);
  }
}

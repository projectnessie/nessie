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

import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.UNIQUE;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.immutables.value.Value;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.UnsafeByteOperations;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/**
 * Describes a unique ID, used to ensure that an ID defined in some {@linkplain #space()} is unique
 * within a Nessie repository.
 */
@Value.Immutable
public interface UniqueIdObj extends Obj {

  @Override
  default ObjType type() {
    return UNIQUE;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  ObjId id();

  @Override
  @Value.Parameter(order = 1)
  @Value.Auxiliary
  long referenced();

  /** The "ID space", for example {@code content-id}. */
  @Value.Parameter(order = 2)
  String space();

  /** The value of the ID within the {@link #space()}. */
  @Value.Parameter(order = 3)
  ByteString value();

  @Value.NonAttribute
  default UUID valueAsUUID() {
    ByteBuffer buffer = value().asReadOnlyByteBuffer();
    long msb = buffer.getLong();
    long lsb = buffer.getLong();
    return new UUID(msb, lsb);
  }

  static UniqueIdObj uniqueId(ObjId id, long referenced, String space, ByteString value) {
    return ImmutableUniqueIdObj.of(id, referenced, space, value);
  }

  static UniqueIdObj uniqueId(String space, ByteString value) {
    return uniqueId(
        objIdHasher(UNIQUE).hash(space).hash(value.asReadOnlyByteBuffer()).generate(),
        0L,
        space,
        value);
  }

  static UniqueIdObj uniqueId(String space, UUID value) {
    return uniqueId(space, uuidToBytes(value));
  }

  static ByteString uuidToBytes(UUID value) {
    ByteBuffer buffer = ByteBuffer.allocate(16);
    buffer.putLong(value.getMostSignificantBits());
    buffer.putLong(value.getLeastSignificantBits());
    buffer.flip();
    return UnsafeByteOperations.unsafeWrap(buffer);
  }
}

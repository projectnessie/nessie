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

import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.REF;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.logic.ReferenceLogic;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/**
 * Describes the <em>internal</em> state of a reference when it has been created, managed by {@link
 * ReferenceLogic} implementations.
 */
@Value.Immutable
public interface RefObj extends Obj {

  @Override
  default ObjType type() {
    return StandardObjType.REF;
  }

  @Value.Parameter(order = 1)
  String name();

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  ObjId id();

  @Override
  @Value.Parameter(order = 1)
  @Value.Auxiliary
  long referenced();

  /**
   * The tip/HEAD of the reference at reference creation. This value does <em>not</em> track the
   * <em>current</em> tip/HEAD of the reference. This value is used to implement create-reference
   * resume/recovery, see implementations of {@link ReferenceLogic}.
   */
  @Value.Parameter(order = 2)
  ObjId initialPointer();

  /** Timestamp when the reference has been created, in microseconds since epoch. */
  @Value.Parameter(order = 3)
  long createdAtMicros();

  @Value.Parameter(order = 4)
  @Nullable
  ObjId extendedInfoObj();

  static RefObj ref(
      ObjId id,
      long referenced,
      String name,
      ObjId initialPointer,
      long createdAtMicros,
      ObjId extendedInfoObj) {
    return ImmutableRefObj.of(
        name, id, referenced, initialPointer, createdAtMicros, extendedInfoObj);
  }

  static RefObj ref(
      String name, ObjId initialPointer, long createdAtMicros, ObjId extendedInfoObj) {
    return ref(
        objIdHasher(REF)
            .hash(name)
            .hash(initialPointer.asByteArray())
            .hash(createdAtMicros)
            .generate(),
        0L,
        name,
        initialPointer,
        createdAtMicros,
        extendedInfoObj);
  }
}

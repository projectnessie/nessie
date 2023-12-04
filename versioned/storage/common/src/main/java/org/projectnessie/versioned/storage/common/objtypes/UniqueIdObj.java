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

import static org.projectnessie.versioned.storage.common.objtypes.Hashes.uniqueIdHash;

import javax.annotation.Nullable;
import org.immutables.value.Value;
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
    return StandardObjType.UNIQUE;
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  @jakarta.annotation.Nullable
  ObjId id();

  /** The "ID space", for example {@code content-id}. */
  @Value.Parameter(order = 2)
  String space();

  /** The value of the ID within the {@link #space()}. */
  @Value.Parameter(order = 3)
  String value();

  static UniqueIdObj uniqueId(ObjId id, String space, String value) {
    return ImmutableUniqueIdObj.of(id, space, value);
  }

  static UniqueIdObj uniqueId(String space, String value) {
    return uniqueId(uniqueIdHash(space, value), space, value);
  }
}

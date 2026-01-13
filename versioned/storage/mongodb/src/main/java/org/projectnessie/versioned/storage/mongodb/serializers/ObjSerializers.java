/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.mongodb.serializers;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;
import org.projectnessie.versioned.storage.common.objtypes.StandardObjType;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public final class ObjSerializers {

  public static final Set<ObjSerializer<?>> ALL_SERIALIZERS =
      Set.of(
          CommitObjSerializer.INSTANCE,
          ContentValueObjSerializer.INSTANCE,
          IndexSegmentsObjSerializer.INSTANCE,
          IndexObjSerializer.INSTANCE,
          RefObjSerializer.INSTANCE,
          StringObjSerializer.INSTANCE,
          TagObjSerializer.INSTANCE,
          UniqueIdObjSerializer.INSTANCE,
          CustomObjSerializer.INSTANCE);

  static {
    Set<String> payloadFieldNames = new HashSet<>();
    ALL_SERIALIZERS.forEach(
        serializer -> {
          String fieldName = requireNonNull(serializer.fieldName());
          if (!payloadFieldNames.add(fieldName)) {
            throw new IllegalStateException("Duplicate field name: " + fieldName);
          }
        });
  }

  @Nonnull
  public static ObjSerializer<Obj> forType(@Nonnull ObjType type) {
    ObjSerializer<?> serializer = CustomObjSerializer.INSTANCE;
    if (type instanceof StandardObjType standardObjType) {
      serializer =
          switch (standardObjType) {
            case COMMIT -> CommitObjSerializer.INSTANCE;
            case INDEX_SEGMENTS -> IndexSegmentsObjSerializer.INSTANCE;
            case INDEX -> IndexObjSerializer.INSTANCE;
            case REF -> RefObjSerializer.INSTANCE;
            case STRING -> StringObjSerializer.INSTANCE;
            case TAG -> TagObjSerializer.INSTANCE;
            case VALUE -> ContentValueObjSerializer.INSTANCE;
            case UNIQUE -> UniqueIdObjSerializer.INSTANCE;
            default -> throw new IllegalArgumentException("Unknown standard object type: " + type);
          };
    }
    @SuppressWarnings("unchecked")
    ObjSerializer<Obj> cast = (ObjSerializer<Obj>) serializer;
    return cast;
  }
}

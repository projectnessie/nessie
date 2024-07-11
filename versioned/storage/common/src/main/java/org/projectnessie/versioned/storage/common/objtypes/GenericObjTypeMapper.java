/*
 * Copyright (C) 2024 Dremio
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

import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.uncachedObjType;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/** <em>Internal</em> class used to handle otherwise unhandled object types. */
public final class GenericObjTypeMapper {
  @Nonnull
  public ObjType mapGenericObjType(String nameOrShortName) {
    return genericTypes.computeIfAbsent(nameOrShortName, GenericObjTypeMapper::newGenericObjType);
  }

  @VisibleForTesting
  public static ObjType newGenericObjType(String name) {
    return uncachedObjType(name, name, GenericObj.class);
  }

  private final Map<String, ObjType> genericTypes = new ConcurrentHashMap<>();
}

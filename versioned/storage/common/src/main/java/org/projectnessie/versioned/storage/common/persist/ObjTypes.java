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
package org.projectnessie.versioned.storage.common.persist;

import static com.google.common.base.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;

public final class ObjTypes {

  /** Returns all registered {@link ObjType}s. */
  @Nonnull
  public static Set<ObjType> allObjTypes() {
    return Registry.OBJ_TYPES;
  }

  @Nonnull
  public static ObjType forName(@Nonnull String name) {
    ObjType type = Registry.BY_NAME.get(name);
    checkArgument(type != null, "Unknown object type name: %s", name);
    return type;
  }

  @Nonnull
  public static ObjType forShortName(@Nonnull String shortName) {
    ObjType type = Registry.BY_SHORT_NAME.get(shortName);
    checkArgument(type != null, "Unknown object type short name: %s", shortName);
    return type;
  }

  private static final class Registry {
    private static final Map<String, ObjType> BY_NAME;
    private static final Map<String, ObjType> BY_SHORT_NAME;
    private static final Set<ObjType> OBJ_TYPES;

    static {
      Map<String, ObjType> byName = new TreeMap<>();
      Map<String, ObjType> byShortName = new HashMap<>();
      for (ObjTypeBundle bundle : ServiceLoader.load(ObjTypeBundle.class)) {
        bundle.register(
            objType -> {
              if (byName.put(objType.name(), objType) != null) {
                throw new IllegalStateException("Duplicate object type name: " + objType.name());
              }
              if (byShortName.put(objType.shortName(), objType) != null) {
                throw new IllegalStateException(
                    "Duplicate object type short name: " + objType.shortName());
              }
            });
      }
      BY_NAME = Collections.unmodifiableMap(byName);
      BY_SHORT_NAME = Collections.unmodifiableMap(byShortName);
      OBJ_TYPES = Set.copyOf(byName.values());
    }
  }
}

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

import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import org.projectnessie.versioned.storage.common.objtypes.GenericObjTypeMapper;

public final class ObjTypes {

  /** Returns all registered {@link ObjType}s. */
  @Nonnull
  public static Set<ObjType> allObjTypes() {
    return Registry.OBJ_TYPES;
  }

  @Nonnull
  public static ObjType objTypeByName(@Nonnull String name) {
    ObjType type = Registry.BY_ANY_NAME.get(name);
    if (type == null) {
      type = Registry.maybeMapped(name);
    }
    return type;
  }

  @Nonnull
  @Deprecated(forRemoval = true)
  public static ObjType forName(@Nonnull String name) {
    return objTypeByName(name);
  }

  @Nonnull
  @Deprecated(forRemoval = true)
  public static ObjType forShortName(@Nonnull String shortName) {
    return objTypeByName(shortName);
  }

  private static final class Registry {
    private static final Map<String, ObjType> BY_ANY_NAME;
    private static final Set<ObjType> OBJ_TYPES;
    private static final GenericObjTypeMapper OBJ_TYPE_MAPPER;

    static ObjType maybeMapped(String name) {
      return OBJ_TYPE_MAPPER.mapGenericObjType(name);
    }

    static {
      Map<String, ObjType> byAnyName = new TreeMap<>();
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
              if (byAnyName.containsKey(objType.name())
                  || byAnyName.containsKey(objType.shortName())) {
                throw new IllegalStateException(
                    "Duplicate object type name/short name: "
                        + objType.name()
                        + "/"
                        + objType.shortName());
              }
              byAnyName.put(objType.name(), objType);
              byAnyName.put(objType.shortName(), objType);
            });
      }
      BY_ANY_NAME = Collections.unmodifiableMap(byAnyName);
      OBJ_TYPES = Set.copyOf(byName.values());
      OBJ_TYPE_MAPPER = new GenericObjTypeMapper();
    }
  }
}

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

import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_ID_KEY;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_TYPE_KEY;
import static org.projectnessie.versioned.storage.common.json.ObjIdHelper.OBJ_VERS_KEY;
import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.uncachedObjType;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;
import org.projectnessie.versioned.storage.common.persist.ObjTypeBundle;
import org.projectnessie.versioned.storage.common.persist.UpdateableObj;

/** Registers a "fallback" handler to handle otherwise unhandled object types. */
public class GenericObjTypeBundle implements ObjTypeBundle, ObjTypeBundle.ObjTypeMapper {
  @Override
  public void register(Consumer<ObjType> registrar) {
    // no own object types
  }

  @Override
  public Optional<ObjTypeMapper> genericObjTypeMapper() {
    return Optional.of(this);
  }

  @Override
  @Nonnull
  public ObjType mapGenericObjType(String nameOrShortName) {
    return genericTypes.computeIfAbsent(nameOrShortName, GenericObjTypeBundle::newGenericObjType);
  }

  @VisibleForTesting
  public static ObjType newGenericObjType(String name) {
    return uncachedObjType(name, name, GenericObj.class);
  }

  private final Map<String, ObjType> genericTypes = new ConcurrentHashMap<>();

  @Value.Immutable
  @Value.Style(jdkOnly = true)
  @JsonSerialize(as = ImmutableGenericObj.class)
  @JsonDeserialize(as = ImmutableGenericObj.class)
  abstract static class GenericObj implements UpdateableObj {
    @Override
    @JsonIgnore
    @JacksonInject(OBJ_TYPE_KEY)
    public abstract ObjType type();

    @Override
    @JsonIgnore
    @JacksonInject(OBJ_ID_KEY)
    public abstract ObjId id();

    @Override
    @JsonIgnore
    @JacksonInject(OBJ_VERS_KEY)
    @Nullable
    public abstract String versionToken();

    @JsonAnyGetter
    @JsonAnySetter
    @AllowNulls
    public abstract Map<String, Object> attributes();
  }

  @Documented
  @Target({ElementType.FIELD, ElementType.METHOD})
  static @interface AllowNulls {}
}

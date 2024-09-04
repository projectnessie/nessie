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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/** A generic {@link Obj} that can be used to store any JSON-serializable object. */
@Value.Immutable
@JsonSerialize(as = ImmutableJsonObj.class)
@JsonDeserialize(as = ImmutableJsonObj.class)
public interface JsonObj extends Obj {

  ObjType TYPE = CustomObjType.customObjType("json", "j", JsonObj.class);

  @Override
  @JsonIgnore
  default ObjType type() {
    return TYPE;
  }

  /** A wrapper for the actual JSON object, to capture type information. */
  @JsonUnwrapped
  JsonBean bean();

  /**
   * Retrieves the bean cast to the given type.
   *
   * @throws ClassCastException if the object cannot be cast to the given type.
   */
  @Value.Derived
  @JsonIgnore
  @Nullable
  default <T> T bean(Class<T> expectedType) {
    return expectedType.cast(bean().object());
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableJsonBean.class)
  @JsonDeserialize(as = ImmutableJsonBean.class)
  interface JsonBean {

    @JsonProperty("t")
    String type();

    @JsonProperty("o")
    @JsonTypeInfo(use = Id.CLASS, include = As.EXTERNAL_PROPERTY, property = "t")
    @Nullable
    Object object();
  }

  /**
   * Creates a {@link JsonObj} for the given bean.
   *
   * <p>This method is NOT suitable for null beans: use either {@link #json(ObjId, long, Class,
   * Object)} or {@link #json(ObjId, long, String, Object)} in order to properly capture the type of
   * the object.
   *
   * <p>This method is NOT suitable either for generic bean types such as {@code MyBean<String>} :
   * use {@link #json(ObjId, long, String, Object)} instead.
   */
  static JsonObj json(ObjId id, long referenced, @Nonnull Object bean) {
    return json(id, referenced, bean.getClass(), bean);
  }

  /**
   * Creates a {@link JsonObj} for the given bean and raw type.
   *
   * <p>For generic types, use {@link #json(ObjId, long, String, Object)} instead.
   */
  static JsonObj json(ObjId id, long referenced, Class<?> type, @Nullable Object bean) {
    return json(id, referenced, type.getName(), bean);
  }

  /**
   * Creates a {@link JsonObj} for the given bean and type.
   *
   * <p>The type must be parseable by Jackson's TypeParser. For generic types, use the following
   * general format: {@code "com.example.MyType<com.example.OtherType>"}.
   */
  static JsonObj json(ObjId id, long referenced, String type, @Nullable Object bean) {
    return ImmutableJsonObj.builder()
        .id(id)
        .referenced(referenced)
        .bean(ImmutableJsonBean.builder().object(bean).type(type).build())
        .build();
  }
}

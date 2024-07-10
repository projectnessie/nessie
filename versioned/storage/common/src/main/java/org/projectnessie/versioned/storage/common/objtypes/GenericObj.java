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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Map;
import org.immutables.value.Value;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

@Value.Immutable
@Value.Style(jdkOnly = true)
@VisibleForTesting
public abstract class GenericObj implements Obj {

  public static final String VERSION_TOKEN_ATTRIBUTE = "versionToken";

  @Override
  @JsonIgnore
  public abstract ObjType type();

  @Override
  @JsonIgnore
  public abstract ObjId id();

  @JsonAnyGetter
  @AllowNulls
  public abstract Map<String, Object> attributes();

  @JsonCreator
  static GenericObj create(
      @JacksonInject(OBJ_TYPE_KEY) ObjType objType,
      @JacksonInject(OBJ_ID_KEY) ObjId id,
      @JacksonInject(OBJ_VERS_KEY) String versionToken,
      @JsonAnySetter Map<String, Object> attributes) {
    ImmutableGenericObj.Builder builder =
        ImmutableGenericObj.builder().type(objType).id(id).attributes(attributes);
    if (versionToken != null) {
      builder.putAttributes(VERSION_TOKEN_ATTRIBUTE, versionToken);
    }
    return builder.build();
  }

  @Documented
  @Target({ElementType.FIELD, ElementType.METHOD})
  @interface AllowNulls {}
}

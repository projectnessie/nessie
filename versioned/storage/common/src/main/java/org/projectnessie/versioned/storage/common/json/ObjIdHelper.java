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
package org.projectnessie.versioned.storage.common.json;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class ObjIdHelper {

  /**
   * The key used to store the injectable {@link ObjId} instance, representing the id of the object
   * being deserialized. Meant to be used in methods and constructor parameters annotated with
   * {@link JacksonInject}.
   */
  public static final String OBJ_ID_KEY = "nessie.storage.ObjId";

  /**
   * Returns an {@link ObjectReader} for the given target {@link Obj} class and with the given
   * {@link ObjId} injectable under the key {@link #OBJ_ID_KEY}.
   */
  public static ObjectReader readerWithObjId(
      ObjectMapper mapper, Class<? extends Obj> targetClass, ObjId id) {
    InjectableValues values = new InjectableValues.Std().addValue(OBJ_ID_KEY, id);
    return mapper.reader(values).forType(targetClass);
  }

  private ObjIdHelper() {}
}

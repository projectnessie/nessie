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
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

public final class ObjIdHelper {

  /**
   * The key used to store the injectable {@link ObjId} instance, representing the id of the object
   * being deserialized. Meant to be used in methods and constructor parameters annotated with
   * {@link JacksonInject}.
   */
  public static final String OBJ_ID_KEY = "nessie.storage.ObjId";

  public static final String OBJ_VERS_KEY = "nessie.storage.ObjVersion";

  public static final String OBJ_TYPE_KEY = "nessie.storage.ObjType";

  public static final String OBJ_REFERENCED_KEY = "nessie.storage.ObjReferenced";

  /**
   * Returns an {@link ObjectReader} for the given target {@link ObjType}, with the given {@link
   * ObjId} injectable under the key {@value #OBJ_ID_KEY}, version token using the key {@value
   * #OBJ_VERS_KEY} and referenced timestamp using the key {@value #OBJ_REFERENCED_KEY}.
   */
  public static ObjectReader contextualReader(
      ObjectMapper mapper, ObjType objType, ObjId id, String objVersionToken, long objReferenced) {
    InjectableValues values =
        new InjectableValues.Std()
            .addValue(OBJ_ID_KEY, id)
            .addValue(OBJ_VERS_KEY, objVersionToken)
            .addValue(OBJ_TYPE_KEY, objType)
            .addValue(OBJ_REFERENCED_KEY, objReferenced);
    return mapper.reader(values).forType(objType.targetClass());
  }

  private ObjIdHelper() {}
}

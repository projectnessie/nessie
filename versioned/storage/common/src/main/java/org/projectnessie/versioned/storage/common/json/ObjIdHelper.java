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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class ObjIdHelper {

  private static final String DESERIALIZATION_CTX_ATTR_KEY = "nessie.storage.ObjId";

  /** Retrieves an {@link ObjId} from the {@link DeserializationContext}. */
  public static ObjId retrieveObjIdFromContext(DeserializationContext ctx) {
    return (ObjId) ctx.getAttribute(DESERIALIZATION_CTX_ATTR_KEY);
  }

  /** Returns an {@link ObjectReader} having the {@link ObjId} stored in its context attributes. */
  public static ObjectReader storeObjIdInContext(ObjectMapper mapper, ObjId id) {
    return mapper.reader().withAttribute(DESERIALIZATION_CTX_ATTR_KEY, id);
  }

  private ObjIdHelper() {}
}

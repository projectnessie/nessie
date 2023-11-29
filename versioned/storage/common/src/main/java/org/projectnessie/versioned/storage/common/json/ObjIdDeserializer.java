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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public class ObjIdDeserializer extends StdDeserializer<ObjId> {

  public ObjIdDeserializer() {
    super(ObjId.class);
  }

  @Override
  public ObjId deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
    return ObjId.objIdFromByteArray(p.getBinaryValue());
  }
}

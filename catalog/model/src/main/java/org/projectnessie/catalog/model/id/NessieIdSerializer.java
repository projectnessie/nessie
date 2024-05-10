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
package org.projectnessie.catalog.model.id;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.type.SimpleType;
import java.io.IOException;

public final class NessieIdSerializer extends JsonSerializer<NessieId> {

  public static final SimpleType BYTE_ARRAY_TYPE = SimpleType.constructUnsafe(byte[].class);

  @Override
  public void serialize(NessieId value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    // TODO serialize as string in JSON, but as bytes for other (binary) formats
    //  can use serializers.getAttribute()
    gen.writeBinary(value.idAsBytes());
  }

  @Override
  public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType type)
      throws JsonMappingException {
    // TODO serialize as string in JSON, but as bytes for other (binary) formats
    //  can use serializers.getAttribute()
    visitor.expectStringFormat(BYTE_ARRAY_TYPE);
  }
}

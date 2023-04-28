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
package org.projectnessie.events.api.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Map;
import org.projectnessie.events.api.CustomEvent;

public final class CustomEventSerializer extends StdSerializer<CustomEvent> {

  public CustomEventSerializer() {
    super(CustomEvent.class);
  }

  @Override
  public void serializeWithType(
      CustomEvent value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
      throws IOException {
    serialize(value, gen, serializers);
  }

  @Override
  public void serialize(CustomEvent value, JsonGenerator gen, SerializerProvider serializers)
      throws IOException {
    gen.writeStartObject();
    gen.writeStringField("id", value.getId().toString());
    gen.writeStringField("type", value.getCustomType());
    gen.writeStringField("repositoryId", value.getRepositoryId());
    gen.writeStringField("createdAt", value.getCreatedAt().toString());
    if (value.getCreatedBy().isPresent()) {
      gen.writeStringField("createdBy", value.getCreatedBy().get());
    }
    for (Map.Entry<String, Object> entry : value.getProperties().entrySet()) {
      gen.writeFieldName(entry.getKey());
      gen.writeObject(entry.getValue());
    }
    gen.writeEndObject();
  }
}

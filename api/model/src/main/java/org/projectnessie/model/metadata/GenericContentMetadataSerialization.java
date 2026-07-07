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
package org.projectnessie.model.metadata;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class GenericContentMetadataSerialization {

  static final class GenericContentMetadataSerializer
      extends JsonSerializer<GenericContentMetadata> {

    @Override
    public void serializeWithType(
        GenericContentMetadata value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSer)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("variant", value.getVariant());
      for (Entry<String, Object> entry : value.getAttributes().entrySet()) {
        gen.writeFieldName(entry.getKey());
        gen.writeObject(entry.getValue());
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(
        GenericContentMetadata value, JsonGenerator gen, SerializerProvider serializers) {
      throw new UnsupportedOperationException();
    }
  }

  static final class GenericContentMetadataDeserializer
      extends JsonDeserializer<GenericContentMetadata> {

    @Override
    public GenericContentMetadata deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, Object> all = p.readValueAs(Map.class);
      Object variant = all.remove("variant");
      if (variant == null) {
        variant = "UNKNOWN_VARIANT";
      }
      return GenericContentMetadata.genericContentMetadata(variant.toString(), all);
    }
  }

  static final class GenericContentMetadataSerializer3
      extends tools.jackson.databind.ValueSerializer<GenericContentMetadata> {

    @Override
    public void serializeWithType(
        GenericContentMetadata value,
        tools.jackson.core.JsonGenerator gen,
        tools.jackson.databind.SerializationContext serializers,
        tools.jackson.databind.jsontype.TypeSerializer typeSer)
        throws tools.jackson.core.JacksonException {
      gen.writeStartObject();
      gen.writeStringProperty("variant", value.getVariant());
      for (Entry<String, Object> entry : value.getAttributes().entrySet()) {
        gen.writePOJOProperty(entry.getKey(), entry.getValue());
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(
        GenericContentMetadata value,
        tools.jackson.core.JsonGenerator gen,
        tools.jackson.databind.SerializationContext serializers) {
      throw new UnsupportedOperationException();
    }
  }

  static final class GenericContentMetadataDeserializer3
      extends tools.jackson.databind.ValueDeserializer<GenericContentMetadata> {

    @Override
    public GenericContentMetadata deserialize(
        tools.jackson.core.JsonParser p, tools.jackson.databind.DeserializationContext ctxt)
        throws tools.jackson.core.JacksonException {
      if (p.currentToken() == tools.jackson.core.JsonToken.START_OBJECT) {
        return fromMap(p.readValueAs(Map.class));
      }

      Map<String, Object> all = new LinkedHashMap<>();
      Object variant = p.getTypeId();
      for (tools.jackson.core.JsonToken token = p.currentToken();
          token == tools.jackson.core.JsonToken.PROPERTY_NAME;
          token = p.nextToken()) {
        String fieldName = p.currentName();
        p.nextToken();
        Object value = p.readValueAs(Object.class);
        if ("variant".equals(fieldName)) {
          variant = value;
        } else {
          all.put(fieldName, value);
        }
      }

      if (variant == null) {
        variant = "UNKNOWN_VARIANT";
      }
      return GenericContentMetadata.genericContentMetadata(variant.toString(), all);
    }

    private GenericContentMetadata fromMap(Map<?, ?> all) {
      Map<String, Object> attributes = new LinkedHashMap<>();
      all.forEach((key, value) -> attributes.put(key.toString(), value));
      Object variant = attributes.remove("variant");
      if (variant == null) {
        variant = "UNKNOWN_VARIANT";
      }
      return GenericContentMetadata.genericContentMetadata(variant.toString(), attributes);
    }
  }
}

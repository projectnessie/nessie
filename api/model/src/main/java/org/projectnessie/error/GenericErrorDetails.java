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
package org.projectnessie.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(using = GenericErrorDetails.GenericTypeSerializer.class)
@tools.jackson.databind.annotation.JsonSerialize(
    using = GenericErrorDetails.GenericTypeSerializer3.class)
@JsonDeserialize(using = GenericErrorDetails.GenericTypeDeserializer.class)
@tools.jackson.databind.annotation.JsonDeserialize(
    using = GenericErrorDetails.GenericTypeDeserializer3.class)
public abstract class GenericErrorDetails implements NessieErrorDetails {
  @Override
  @Value.Parameter(order = 1)
  public abstract String getType();

  @Nullable
  @jakarta.annotation.Nullable
  @Schema(type = SchemaType.OBJECT)
  @Value.Parameter(order = 2)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonUnwrapped
  public abstract Map<String, Object> getAttributes();

  static final class GenericTypeSerializer extends JsonSerializer<GenericErrorDetails> {

    @Override
    public void serializeWithType(
        GenericErrorDetails value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSer)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", value.getType());
      for (Map.Entry<String, Object> entry : value.getAttributes().entrySet()) {
        gen.writeFieldName(entry.getKey());
        gen.writeObject(entry.getValue());
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(
        GenericErrorDetails value, JsonGenerator gen, SerializerProvider serializers) {
      throw new UnsupportedOperationException();
    }
  }

  static final class GenericTypeDeserializer extends JsonDeserializer<GenericErrorDetails> {

    @Override
    public GenericErrorDetails deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, Object> all = p.readValueAs(Map.class);
      Object type = all.remove("type");
      if (type == null) {
        type = "UNKNOWN_ERROR_DETAILS";
      }
      return GenericErrorDetails.errorUnknownType(type.toString(), all);
    }
  }

  static final class GenericTypeSerializer3
      extends tools.jackson.databind.ValueSerializer<GenericErrorDetails> {

    @Override
    public void serializeWithType(
        GenericErrorDetails value,
        tools.jackson.core.JsonGenerator gen,
        tools.jackson.databind.SerializationContext serializers,
        tools.jackson.databind.jsontype.TypeSerializer typeSer)
        throws tools.jackson.core.JacksonException {
      gen.writeStartObject();
      gen.writeStringProperty("type", value.getType());
      for (Map.Entry<String, Object> entry : value.getAttributes().entrySet()) {
        gen.writePOJOProperty(entry.getKey(), entry.getValue());
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(
        GenericErrorDetails value,
        tools.jackson.core.JsonGenerator gen,
        tools.jackson.databind.SerializationContext serializers) {
      throw new UnsupportedOperationException();
    }
  }

  static final class GenericTypeDeserializer3
      extends tools.jackson.databind.ValueDeserializer<GenericErrorDetails> {

    @Override
    public GenericErrorDetails deserialize(
        tools.jackson.core.JsonParser p, tools.jackson.databind.DeserializationContext ctxt)
        throws tools.jackson.core.JacksonException {
      if (p.currentToken() == tools.jackson.core.JsonToken.START_OBJECT) {
        return fromMap(p.readValueAs(Map.class));
      }

      Map<String, Object> all = new LinkedHashMap<>();
      Object type = p.getTypeId();
      for (tools.jackson.core.JsonToken token = p.currentToken();
          token == tools.jackson.core.JsonToken.PROPERTY_NAME;
          token = p.nextToken()) {
        String fieldName = p.currentName();
        p.nextToken();
        Object value = p.readValueAs(Object.class);
        if ("type".equals(fieldName)) {
          type = value;
        } else {
          all.put(fieldName, value);
        }
      }

      if (type == null) {
        type = "UNKNOWN_ERROR_DETAILS";
      }
      return GenericErrorDetails.errorUnknownType(type.toString(), all);
    }

    private GenericErrorDetails fromMap(Map<?, ?> all) {
      Map<String, Object> attributes = new LinkedHashMap<>();
      all.forEach((key, value) -> attributes.put(key.toString(), value));
      Object type = attributes.remove("type");
      if (type == null) {
        type = "UNKNOWN_ERROR_DETAILS";
      }
      return GenericErrorDetails.errorUnknownType(type.toString(), attributes);
    }
  }

  public static GenericErrorDetails errorUnknownType(String type, Map<String, Object> attributes) {
    return ImmutableGenericErrorDetails.of(type, attributes);
  }
}

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
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(using = GenericErrorDetails.GenericTypeSerializer.class)
@JsonDeserialize(using = GenericErrorDetails.GenericTypeDeserializer.class)
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

  public static GenericErrorDetails errorUnknownType(String type, Map<String, Object> attributes) {
    return ImmutableGenericErrorDetails.of(type, attributes);
  }
}

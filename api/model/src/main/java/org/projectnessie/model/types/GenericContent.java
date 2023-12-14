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
package org.projectnessie.model.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
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
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.Content;

/**
 * Special {@link Content} reserved for cases when the actual {@link Content#getType() content type}
 * is not available.
 *
 * <p>Nessie servers cannot properly handle unknown {@link Content#getType() content types}, but
 * with this "fallback" clients can at least deserialize the content object and do not fail hard /
 * error out.
 */
@Value.Immutable
@JsonSerialize(using = GenericContent.ContentUnknownTypeSerializer.class)
@JsonDeserialize(using = GenericContent.ContentUnknownTypeDeserializer.class)
public abstract class GenericContent extends Content {

  @Override
  @Value.Parameter(order = 1)
  public abstract Content.Type getType();

  @Nullable
  @Override
  @Value.Parameter(order = 2)
  public abstract String getId();

  @Nullable
  @Schema(type = SchemaType.OBJECT)
  @Value.Parameter(order = 3)
  @JsonInclude(Include.NON_NULL)
  @JsonUnwrapped
  public abstract Map<String, Object> getAttributes();

  static final class ContentUnknownTypeSerializer extends JsonSerializer<GenericContent> {

    @Override
    public void serializeWithType(
        GenericContent value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSer)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", value.getType().name());
      String id = value.getId();
      if (id != null) {
        gen.writeStringField("id", id);
      }
      for (Entry<String, Object> entry : value.getAttributes().entrySet()) {
        gen.writeFieldName(entry.getKey());
        gen.writeObject(entry.getValue());
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(GenericContent value, JsonGenerator gen, SerializerProvider serializers) {
      throw new UnsupportedOperationException();
    }
  }

  static final class ContentUnknownTypeDeserializer extends JsonDeserializer<GenericContent> {

    @Override
    public GenericContent deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, Object> all = p.readValueAs(Map.class);
      Object type = all.remove("type");
      Object id = all.remove("id");
      if (type == null) {
        type = "UNKNOWN_CONTENT_TYPE";
      }
      return GenericContent.contentUnknownType(
          type.toString(), id != null ? id.toString() : null, all);
    }
  }

  public static GenericContent contentUnknownType(String type, String id, Map<String, Object> all) {
    return ImmutableGenericContent.of(
        new Content.Type() {
          @Override
          public String name() {
            return type;
          }

          @Override
          public Class<? extends Content> type() {
            return GenericContent.class;
          }
        },
        id,
        all);
  }
}

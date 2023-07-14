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
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;
import org.projectnessie.model.RepositoryConfig;

/**
 * Special {@link org.projectnessie.model.RepositoryConfig} reserved for cases when the actual
 * {@link RepositoryConfig#getType() repository config type} is not available.
 *
 * <p>Nessie servers cannot properly handle unknown {@link RepositoryConfig#getType() repository
 * config types}, but with this "fallback" clients can at least deserialize the repository config
 * object and do not fail hard / error out.
 */
@Value.Immutable
@JsonSerialize(using = GenericRepositoryConfig.RepositoryConfigUnknownTypeSerializer.class)
@JsonDeserialize(using = GenericRepositoryConfig.RepositoryConfigUnknownTypeDeserializer.class)
public abstract class GenericRepositoryConfig implements RepositoryConfig {

  @Override
  @Value.Parameter(order = 1)
  public abstract RepositoryConfig.Type getType();

  @Nullable
  @jakarta.annotation.Nullable
  @Schema(type = SchemaType.OBJECT)
  @Value.Parameter(order = 3)
  @JsonInclude(Include.NON_NULL)
  @JsonUnwrapped
  public abstract Map<String, Object> getAttributes();

  static final class RepositoryConfigUnknownTypeSerializer
      extends JsonSerializer<GenericRepositoryConfig> {

    @Override
    public void serializeWithType(
        GenericRepositoryConfig value,
        JsonGenerator gen,
        SerializerProvider serializers,
        TypeSerializer typeSer)
        throws IOException {
      gen.writeStartObject();
      gen.writeStringField("type", value.getType().name());
      for (Entry<String, Object> entry : value.getAttributes().entrySet()) {
        gen.writeFieldName(entry.getKey());
        gen.writeObject(entry.getValue());
      }
      gen.writeEndObject();
    }

    @Override
    public void serialize(
        GenericRepositoryConfig value, JsonGenerator gen, SerializerProvider serializers) {
      throw new UnsupportedOperationException();
    }
  }

  static final class RepositoryConfigUnknownTypeDeserializer
      extends JsonDeserializer<GenericRepositoryConfig> {

    @Override
    public GenericRepositoryConfig deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      @SuppressWarnings("unchecked")
      Map<String, Object> all = p.readValueAs(Map.class);
      Object type = all.remove("type");
      if (type == null) {
        type = "UNKNOWN_CONTENT_TYPE";
      }
      return GenericRepositoryConfig.repositoryConfigUnknownType(type.toString(), all);
    }
  }

  public static GenericRepositoryConfig repositoryConfigUnknownType(
      String type, Map<String, Object> all) {
    return ImmutableGenericRepositoryConfig.of(
        new RepositoryConfig.Type() {
          @Override
          public String name() {
            return type;
          }

          @Override
          public Class<? extends RepositoryConfig> type() {
            return GenericRepositoryConfig.class;
          }
        },
        all);
  }
}

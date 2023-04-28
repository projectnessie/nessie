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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.projectnessie.events.api.GenericEvent;
import org.projectnessie.events.api.ImmutableGenericEvent;

public final class GenericEventDeserializer extends StdDeserializer<GenericEvent> {

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  public GenericEventDeserializer() {
    super(GenericEvent.class);
  }

  @Override
  public GenericEvent deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
    try {
      Map<String, Object> properties = p.readValueAs(MAP_TYPE);
      Object id = Objects.requireNonNull(properties.remove("id"));
      Object genericType = Objects.requireNonNull(properties.remove("type"));
      Object repositoryId = Objects.requireNonNull(properties.remove("repositoryId"));
      Object createdAt = Objects.requireNonNull(properties.remove("createdAt"));
      Object createdBy = properties.remove("createdBy");
      return ImmutableGenericEvent.builder()
          .id(UUID.fromString(id.toString()))
          .genericType(genericType.toString())
          .repositoryId(repositoryId.toString())
          .createdAt(Instant.parse(createdAt.toString()))
          .createdBy(Optional.ofNullable(createdBy).map(Object::toString))
          .properties(properties)
          .build();
    } catch (Exception e) {
      throw JsonMappingException.from(ctx, "Failed to deserialize GenericEvent", e);
    }
  }
}

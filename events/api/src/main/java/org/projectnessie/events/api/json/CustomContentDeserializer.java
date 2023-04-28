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
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.projectnessie.events.api.CustomContent;
import org.projectnessie.events.api.ImmutableCustomContent;

public final class CustomContentDeserializer extends StdDeserializer<CustomContent> {

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  public CustomContentDeserializer() {
    super(CustomContent.class);
  }

  @Override
  public CustomContent deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
    try {
      Map<String, Object> properties = p.readValueAs(MAP_TYPE);
      Object id = Objects.requireNonNull(properties.remove("id"));
      Object customType = Objects.requireNonNull(properties.remove("type"));
      return ImmutableCustomContent.builder()
          .id(id.toString())
          .customType(customType.toString())
          .properties(properties)
          .build();
    } catch (Exception e) {
      throw InvalidDefinitionException.from(ctx, "Failed to deserialize CustomContent", e);
    }
  }
}

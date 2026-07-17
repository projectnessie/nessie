/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.tasks.api;

import java.time.Instant;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;

public final class JacksonSerializers {
  private JacksonSerializers() {}

  public static final class InstantAsLongDeserializer extends ValueDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
      if (p.currentToken().isNumeric()) {
        long millis = p.getValueAsLong();
        return Instant.ofEpochMilli(millis);
      }
      return null;
    }
  }

  public static final class InstantAsLongSerializer extends ValueSerializer<Instant> {
    @Override
    public void serialize(Instant value, JsonGenerator gen, SerializationContext serializers)
        throws JacksonException {
      if (value == null) {
        gen.writeNull();
      } else {
        gen.writeNumber(value.toEpochMilli());
      }
    }
  }
}

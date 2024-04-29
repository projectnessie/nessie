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
package org.projectnessie.client.auth.oauth2;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.Duration;

class JacksonSerializers {

  private JacksonSerializers() {}

  static class DurationToSecondsSerializer extends StdSerializer<Duration> {

    public DurationToSecondsSerializer() {
      super(Duration.class);
    }

    @Override
    public void serialize(Duration value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      if (value == null) {
        gen.writeNull();
      } else {
        gen.writeNumber(value.getSeconds());
      }
    }
  }

  static class SecondsToDurationDeserializer extends StdDeserializer<Duration> {

    public SecondsToDurationDeserializer() {
      super(Duration.class);
    }

    @Override
    public Duration deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
      if (p.currentToken().isNumeric()) {
        int seconds = p.getValueAsInt();
        if (seconds != 0) {
          return Duration.ofSeconds(seconds);
        }
      }
      return null;
    }
  }
}

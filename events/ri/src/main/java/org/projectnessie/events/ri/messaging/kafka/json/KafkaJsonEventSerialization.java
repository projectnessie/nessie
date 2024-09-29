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
package org.projectnessie.events.ri.messaging.kafka.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.ri.messaging.MessageHeaders;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;
import org.projectnessie.model.ser.Views.V1;
import org.projectnessie.model.ser.Views.V2;

public class KafkaJsonEventSerialization {

  static {
    int apiVersion = NessieConfiguration.getBuiltInConfig().getMaxSupportedApiVersion();
    if (apiVersion != 2) {
      throw new IllegalStateException(
          "Unexpected API version, this class might be outdated: " + apiVersion);
    }
  }

  public static class Serializer
      implements org.apache.kafka.common.serialization.Serializer<Event> {

    private final ObjectMapper objectMapper;

    public Serializer() {
      objectMapper = CDI.current().select(ObjectMapper.class).get();
    }

    @Override
    public byte[] serialize(String topic, Event data) {
      return serializeWithView(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Event data) {
      return serializeWithView(data);
    }

    private byte[] serializeWithView(Event data) {
      if (data == null) {
        return null;
      }
      try {
        return objectMapper.writerWithView(Views.V2.class).writeValueAsBytes(data);
      } catch (Exception e) {
        throw new SerializationException("Error serializing JSON message", e);
      }
    }
  }

  /** Not used by production code, but used in tests. */
  public static class Deserializer
      implements org.apache.kafka.common.serialization.Deserializer<Event> {

    private final ObjectMapper objectMapper;

    public Deserializer() {
      objectMapper = CDI.current().select(ObjectMapper.class).get();
    }

    @Override
    public Event deserialize(String topic, byte[] data) {
      if (data == null) {
        return null;
      }
      return deserializeWithView(data, 2);
    }

    @Override
    public Event deserialize(String topic, Headers headers, byte[] data) {
      if (data == null) {
        return null;
      }
      Header apiVersionHeader = headers.lastHeader(MessageHeaders.API_VERSION.key());
      byte[] value = apiVersionHeader == null ? null : apiVersionHeader.value();
      int apiVersion = value == null || value.length == 0 ? 2 : value[0] - '0';
      return deserializeWithView(data, apiVersion);
    }

    private Event deserializeWithView(byte[] data, int apiVersion) {
      try {
        Class<?> view =
            switch (apiVersion) {
              case 1 -> V1.class;
              case 2 -> V2.class;
              default ->
                  throw new IllegalArgumentException("Unsupported API version: " + apiVersion);
            };
        return objectMapper.readerWithView(view).readValue(data, Event.class);
      } catch (Exception e) {
        throw new SerializationException("Error deserializing JSON message", e);
      }
    }
  }
}

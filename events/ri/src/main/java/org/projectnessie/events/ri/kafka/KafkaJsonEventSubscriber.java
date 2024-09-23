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
package org.projectnessie.events.ri.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.model.ser.Views.V1;
import org.projectnessie.model.ser.Views.V2;

/** An {@link EventSubscriber} that publishes events to a Kafka topic using JSON. */
public class KafkaJsonEventSubscriber extends AbstractKafkaEventSubscriber<Event> {

  public KafkaJsonEventSubscriber() throws UncheckedIOException {}

  public KafkaJsonEventSubscriber(String location) throws UncheckedIOException {
    super(location);
  }

  public KafkaJsonEventSubscriber(Properties props) {
    super(props);
  }

  public KafkaJsonEventSubscriber(
      Properties props, Function<Properties, Producer<String, Event>> producerFactory) {
    super(props, producerFactory);
  }

  @Override
  public void onEvent(Event event) {
    fireEvent(event, event);
  }

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().findAndRegisterModules();

  /**
   * Alternative to {@code KafkaJsonSerializer} that uses the correct view for serialization of
   * Nessie events.
   */
  public static class NessieEventsJsonSerializer implements Serializer<Event> {

    @Override
    public byte[] serialize(String topic, Event data) {
      return serializeWithView(data, 2);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Event data) {
      byte[] value = Header.MAX_API_VERSION.getValue(headers);
      int apiVersion = value == null || value.length == 0 ? 2 : value[0];
      return serializeWithView(data, apiVersion);
    }

    private byte[] serializeWithView(Event data, int apiVersion) {
      if (data == null) {
        return null;
      }
      try {
        Class<?> view = apiVersion == 2 ? V2.class : V1.class;
        return OBJECT_MAPPER.writerWithView(view).writeValueAsBytes(data);
      } catch (Exception e) {
        throw new SerializationException("Error serializing JSON message", e);
      }
    }
  }

  /**
   * Alternative to {@code KafkaJsonDeserializer} that uses the correct view for deserialization of
   * Nessie events.
   */
  public static class NessieEventsJsonDeserializer implements Deserializer<Event> {

    @Override
    public Event deserialize(String topic, byte[] data) {
      return deserializeWithView(data, 2);
    }

    @Override
    public Event deserialize(String topic, Headers headers, byte[] data) {
      byte[] value = Header.MAX_API_VERSION.getValue(headers);
      int apiVersion = value == null || value.length == 0 ? 2 : value[0];
      return deserializeWithView(data, apiVersion);
    }

    private Event deserializeWithView(byte[] data, int apiVersion) {
      if (data == null) {
        return null;
      }
      try {
        Class<?> view = apiVersion == 2 ? V2.class : V1.class;
        return OBJECT_MAPPER.readerWithView(view).readValue(data, Event.class);
      } catch (Exception e) {
        throw new SerializationException("Error deserializing JSON message", e);
      }
    }
  }
}

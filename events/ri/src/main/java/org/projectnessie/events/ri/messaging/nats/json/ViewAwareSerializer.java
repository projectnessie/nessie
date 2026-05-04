/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.events.ri.messaging.nats.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkiverse.reactive.messaging.nats.jetstream.client.mapper.Serializer;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.io.IOException;
import org.projectnessie.model.ser.Views;

// This type does effectively the same thing as the old ViewAwareJsonPayloadMapper.
@ApplicationScoped
@Alternative // Want to replace
// io.quarkiverse.reactive.messaging.nats.jetstream.client.mapper.SerializerImpl
public class ViewAwareSerializer implements Serializer {
  private final ObjectMapper objectMapper;

  public ViewAwareSerializer(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public <T> byte[] toBytes(T payload) {
    try {
      if (payload == null) {
        return new byte[0];
      } else if (payload instanceof byte[] bb) {
        return bb;
      } else {
        assert objectMapper != null;
        return objectMapper.writerWithView(Views.V2.class).writeValueAsBytes(payload);
      }
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T readValue(byte[] data, Class<T> type) {
    try {
      assert objectMapper != null;
      return objectMapper.readerWithView(Views.V2.class).readValue(data, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

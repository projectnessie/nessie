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
package org.projectnessie.events.ri.messaging.nats.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkiverse.reactive.messaging.nats.jetstream.mapper.DefaultPayloadMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;

@ApplicationScoped
public class ViewAwareJsonPayloadMapper extends DefaultPayloadMapper {

  static {
    int apiVersion = NessieConfiguration.getBuiltInConfig().getMaxSupportedApiVersion();
    if (apiVersion != 2) {
      throw new IllegalStateException(
          "Unexpected API version, this class might be outdated: " + apiVersion);
    }
  }

  private final ObjectMapper objectMapper;

  @SuppressWarnings("unused")
  public ViewAwareJsonPayloadMapper() {
    super(null);
    this.objectMapper = null;
  }

  @Inject
  public ViewAwareJsonPayloadMapper(ObjectMapper objectMapper) {
    super(objectMapper);
    this.objectMapper = objectMapper;
  }

  @Override
  public byte[] of(Object payload) {
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
  public <T> T of(byte[] data, Class<T> type) {
    try {
      assert objectMapper != null;
      return objectMapper.readerWithView(Views.V2.class).readValue(data, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

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
import io.quarkiverse.reactive.messaging.nats.jetstream.client.message.MessageFactory;
import io.quarkiverse.reactive.messaging.nats.jetstream.tracing.JetStreamInstrumenter;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import java.io.IOException;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.model.ser.Views;

@Alternative
@Priority(1)
@ApplicationScoped
public class ViewAwareJsonMessageFactory extends MessageFactory {

  static {
    int apiVersion = NessieConfiguration.getBuiltInConfig().getMaxSupportedApiVersion();
    if (apiVersion != 2) {
      throw new IllegalStateException(
          "Unexpected API version, this class might be outdated: " + apiVersion);
    }
  }

  private final ObjectMapper objectMapper;

  @SuppressWarnings("unused")
  public ViewAwareJsonMessageFactory() {
    super(null, null);
    this.objectMapper = null;
  }

  @Inject
  public ViewAwareJsonMessageFactory(
      ObjectMapper objectMapper, JetStreamInstrumenter instrumenter) {
    super(objectMapper, instrumenter);
    this.objectMapper = objectMapper;
  }

  @Override
  public byte[] toByteArray(Object payload) {
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

  // Not used in production code, only in tests
  @Override
  public <T> T decode(byte[] data, Class<T> type) {
    try {
      assert objectMapper != null;
      return objectMapper.readerWithView(Views.V2.class).readValue(data, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

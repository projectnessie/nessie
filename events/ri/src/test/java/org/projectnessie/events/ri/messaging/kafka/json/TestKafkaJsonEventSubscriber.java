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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.ri.messaging.kafka.AbstractKafkaEventSubscriberTests;
import org.projectnessie.events.spi.EventSubscriber;

@QuarkusTest
public class TestKafkaJsonEventSubscriber extends AbstractKafkaEventSubscriberTests<Event> {

  @Inject KafkaJsonEventSubscriber subscriber;

  @Override
  protected EventSubscriber subscriber() {
    return subscriber;
  }

  @Override
  protected Message<Event> receive() {
    return consumer.awaitKafkaJsonMessages(1).getFirst();
  }

  @Override
  protected void checkMessage(Message<Event> actual, Event upstreamEvent) {
    super.checkMessage(actual, upstreamEvent);
    assertThat(actual.getPayload()).isEqualTo(upstreamEvent);
  }
}

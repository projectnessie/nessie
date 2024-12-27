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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.projectnessie.events.ri.messaging.MessageHeaders.API_VERSION;
import static org.projectnessie.events.ri.messaging.MessageHeaders.COMMIT_CREATION_TIME;
import static org.projectnessie.events.ri.messaging.MessageHeaders.EVENT_CREATION_TIME;
import static org.projectnessie.events.ri.messaging.MessageHeaders.EVENT_ID;
import static org.projectnessie.events.ri.messaging.MessageHeaders.EVENT_TYPE;
import static org.projectnessie.events.ri.messaging.MessageHeaders.INITIATOR;
import static org.projectnessie.events.ri.messaging.MessageHeaders.REPOSITORY_ID;
import static org.projectnessie.events.ri.messaging.MessageHeaders.SPEC_VERSION;

import io.quarkiverse.reactive.messaging.nats.jetstream.client.api.SubscribeMessageMetadata;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.ri.messaging.AbstractMessagingEventSubscriberTests;
import org.projectnessie.events.ri.messaging.nats.AbstractNatsEventSubscriber;
import org.projectnessie.events.spi.EventSubscriber;

@QuarkusTest
public class TestNatsJsonEventSubscriber extends AbstractMessagingEventSubscriberTests<Event> {

  @Inject NatsJsonEventSubscriber subscriber;

  @Override
  protected EventSubscriber subscriber() {
    return subscriber;
  }

  @Override
  protected Message<Event> receive() {
    return consumer.awaitNatsMessages(1).getFirst();
  }

  @Override
  protected void checkMessage(Message<Event> message, Event expectedPayload) {
    SubscribeMessageMetadata metadata =
        message.getMetadata().get(SubscribeMessageMetadata.class).orElseThrow();
    assertThat(metadata.messageId()).isEqualTo(expectedPayload.getIdAsText());
    String subject = AbstractNatsEventSubscriber.subject(expectedPayload);
    assertThat(metadata.subject()).isEqualTo(subject);
    checkHeaders(metadata.headers(), expectedPayload);
    assertThat(message.getPayload()).isEqualTo(expectedPayload);
  }

  protected void checkHeaders(Map<String, List<String>> actual, Event event) {
    assertThat(actual.entrySet())
        .extracting(Map.Entry::getKey, entry -> entry.getValue().getLast())
        .contains(
            tuple(EVENT_ID.key(), event.getIdAsText()),
            tuple(EVENT_TYPE.key(), event.getType().name()),
            tuple(SPEC_VERSION.key(), "2.0.0"),
            tuple(API_VERSION.key(), "2"),
            tuple(REPOSITORY_ID.key(), "repo1"),
            tuple(INITIATOR.key(), "alice"),
            tuple(EVENT_CREATION_TIME.key(), now.toString()));
    if (event.getType() == EventType.COMMIT
        || event.getType() == EventType.CONTENT_REMOVED
        || event.getType() == EventType.CONTENT_STORED) {
      assertThat(actual.get(COMMIT_CREATION_TIME.key()).getLast()).isEqualTo(now.toString());
    }
  }
}

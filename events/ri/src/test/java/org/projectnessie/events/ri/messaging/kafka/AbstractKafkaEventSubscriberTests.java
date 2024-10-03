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
package org.projectnessie.events.ri.messaging.kafka;

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

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.ri.messaging.AbstractMessagingEventSubscriberTests;

public abstract class AbstractKafkaEventSubscriberTests<T>
    extends AbstractMessagingEventSubscriberTests<T> {

  @Override
  protected void checkMessage(Message<T> actual, Event upstreamEvent) {
    IncomingKafkaRecordMetadata<?, ?> metadata =
        actual.getMetadata().get(IncomingKafkaRecordMetadata.class).orElseThrow();
    assertThat(metadata.getKey()).isEqualTo(AbstractKafkaEventSubscriber.recordKey(upstreamEvent));
    checkHeaders(metadata.getHeaders(), upstreamEvent);
  }

  protected void checkHeaders(Headers actual, Event event) {
    assertThat(actual)
        .extracting(Header::key, h -> new String(h.value(), StandardCharsets.UTF_8))
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
      assertThat(actual.lastHeader(COMMIT_CREATION_TIME.key()).value())
          .isEqualTo(now.toString().getBytes(StandardCharsets.UTF_8));
    }
  }
}

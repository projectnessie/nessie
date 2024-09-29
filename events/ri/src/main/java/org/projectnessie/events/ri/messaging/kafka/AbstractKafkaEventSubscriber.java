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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MultiReferenceEvent;
import org.projectnessie.events.api.ReferenceEvent;
import org.projectnessie.events.ri.messaging.AbstractMessagingEventSubscriber;
import org.projectnessie.events.ri.messaging.MessageHeaders;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig.EventSubscriberConfig;
import org.projectnessie.events.spi.EventSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EventSubscriber} that publishes events to a Kafka topic.
 *
 * @param <T> The Kafka record value type emitted by this subscriber.
 */
public abstract class AbstractKafkaEventSubscriber<T> extends AbstractMessagingEventSubscriber<T>
    implements EventSubscriber {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaEventSubscriber.class);

  public AbstractKafkaEventSubscriber(Emitter<T> emitter, EventSubscriberConfig config) {
    super(emitter, config);
  }

  @Override
  protected Message<T> createMessage(Event upstreamEvent, T messagePayload) {
    RecordHeaders headers = new RecordHeaders();
    createHeaders(upstreamEvent, (name, value) -> headers.add(name, value.getBytes(UTF_8)));
    OutgoingKafkaRecordMetadata<String> metadata =
        OutgoingKafkaRecordMetadata.<String>builder()
            .withKey(recordKey(upstreamEvent))
            .withHeaders(headers)
            .build();
    return Message.of(messagePayload, Metadata.of(metadata));
  }

  @Override
  protected CompletionStage<Void> onWriteAck(Metadata metadata) {
    // Do NOT enable this log statement in production!
    if (LOGGER.isDebugEnabled()) {
      OutgoingKafkaRecordMetadata<?> kafkaMetadata =
          metadata.get(OutgoingKafkaRecordMetadata.class).orElseThrow();
      Object key = kafkaMetadata.getKey();
      Header eventIdHeader = kafkaMetadata.getHeaders().lastHeader(MessageHeaders.EVENT_ID.key());
      String eventId = new String(eventIdHeader.value(), UTF_8);
      LOGGER.debug("Event written to Kafka: key={}, id={}", key, eventId);
    }
    return CompletableFuture.completedFuture(null); // immediate ack
  }

  @Override
  protected CompletionStage<Void> onWriteNack(Throwable error, Metadata metadata) {
    OutgoingKafkaRecordMetadata<?> kafkaMetadata =
        metadata.get(OutgoingKafkaRecordMetadata.class).orElseThrow();
    Object key = kafkaMetadata.getKey();
    Header eventIdHeader = kafkaMetadata.getHeaders().lastHeader(MessageHeaders.EVENT_ID.key());
    String eventId = new String(eventIdHeader.value(), UTF_8);
    LOGGER.error("Failed to write event to Kafka: key={}, id={}", key, eventId, error);
    return CompletableFuture.completedFuture(null); // immediate ack
  }

  /**
   * Create a key for the downstream event. This is a combination of the repository ID and the
   * reference name. This allows the topic to be partitioned by repository ID and reference name,
   * which should help with downstream consumers that want to process events for a given reference
   * in order.
   *
   * <p>Note: strict ordering of events is not guaranteed. If necessary, consider using Kafka
   * Streams.
   */
  public static String recordKey(Event event) {
    String repositoryId = event.getRepositoryId();
    String reference;
    if (event instanceof ReferenceEvent) {
      reference = ((ReferenceEvent) event).getReference().getName();
    } else {
      assert event instanceof MultiReferenceEvent;
      reference = ((MultiReferenceEvent) event).getTargetReference().getName();
    }
    return repositoryId + ":" + reference;
  }
}

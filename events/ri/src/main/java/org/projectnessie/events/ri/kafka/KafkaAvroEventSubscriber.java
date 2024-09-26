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

import com.example.nessie.events.generated.CommitEvent;
import com.example.nessie.events.generated.OperationEvent;
import com.example.nessie.events.generated.OperationEventType;
import com.example.nessie.events.generated.ReferenceEvent;
import com.example.nessie.events.generated.ReferenceEventType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.Producer;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventTypeFilter;

/** An {@link EventSubscriber} that publishes events to a Kafka topic using Avro. */
public class KafkaAvroEventSubscriber extends AbstractKafkaEventSubscriber<Object> {

  // Accept all event types except: MERGE and TRANSPLANT (as an example of event type filter)
  private static final EventTypeFilter EVENT_TYPE_FILTER =
      EventTypeFilter.of(
          EventType.COMMIT,
          EventType.REFERENCE_CREATED,
          EventType.REFERENCE_DELETED,
          EventType.REFERENCE_UPDATED,
          EventType.CONTENT_STORED,
          EventType.CONTENT_REMOVED);

  /**
   * Default no-arg constructor.
   *
   * <p>A default no-arg constructor MUST be present in every {@link EventSubscriber}
   * implementation, since that's the constructor used by {@link java.util.ServiceLoader}.
   */
  public KafkaAvroEventSubscriber() throws UncheckedIOException {}

  public KafkaAvroEventSubscriber(String location) throws UncheckedIOException {
    super(location);
  }

  public KafkaAvroEventSubscriber(Properties props) {
    super(props);
  }

  public KafkaAvroEventSubscriber(
      Properties props, Function<Properties, Producer<String, Object>> producerFactory) {
    super(props, producerFactory);
  }

  @Override
  public final EventTypeFilter getEventTypeFilter() {
    return EVENT_TYPE_FILTER;
  }

  @Override
  public void onCommit(org.projectnessie.events.api.CommitEvent upstreamEvent) {
    CommitEvent downstreamEvent =
        new CommitEvent(
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHashBefore(),
            upstreamEvent.getHashAfter());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onContentStored(org.projectnessie.events.api.ContentStoredEvent upstreamEvent) {
    OperationEvent downstreamEvent =
        new OperationEvent(
            OperationEventType.PUT,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHash(),
            upstreamEvent.getContentKey().toCanonicalString(),
            upstreamEvent.getContent().getId(),
            upstreamEvent.getContent().getType().toString(),
            objectAsMap(upstreamEvent.getContent(), "id", "type"));
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onContentRemoved(org.projectnessie.events.api.ContentRemovedEvent upstreamEvent) {
    OperationEvent downstreamEvent =
        new OperationEvent(
            OperationEventType.DELETE,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHash(),
            upstreamEvent.getContentKey().toCanonicalString(),
            null,
            null,
            Map.of());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onReferenceCreated(org.projectnessie.events.api.ReferenceCreatedEvent upstreamEvent) {
    ReferenceEvent downstreamEvent =
        new ReferenceEvent(
            ReferenceEventType.CREATED,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            null,
            upstreamEvent.getHashAfter());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onReferenceUpdated(org.projectnessie.events.api.ReferenceUpdatedEvent upstreamEvent) {
    ReferenceEvent downstreamEvent =
        new ReferenceEvent(
            ReferenceEventType.REASSIGNED,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHashBefore(),
            upstreamEvent.getHashAfter());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onReferenceDeleted(org.projectnessie.events.api.ReferenceDeletedEvent upstreamEvent) {
    ReferenceEvent downstreamEvent =
        new ReferenceEvent(
            ReferenceEventType.DELETED,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHashBefore(),
            null);
    fireEvent(upstreamEvent, downstreamEvent);
  }

  private static final ObjectMapper MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private static Map<String, String> objectAsMap(Object o, String... ignoredProperties) {
    Set<String> ignoredSet = Set.of(ignoredProperties);
    return MAPPER.convertValue(o, MAP_TYPE).entrySet().stream()
        .filter(e -> !ignoredSet.contains(e.getKey()))
        .filter(e -> e.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
  }
}

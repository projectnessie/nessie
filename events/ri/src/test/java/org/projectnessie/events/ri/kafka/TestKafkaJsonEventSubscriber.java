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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.COMMIT_CREATION_TIME;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.EVENT_CREATION_TIME;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.EVENT_TYPE;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.INITIATOR;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.MAX_API_VERSION;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.REPOSITORY_ID;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.SPEC_VERSION;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableMergeEvent;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.ImmutableReferenceUpdatedEvent;
import org.projectnessie.events.api.ImmutableTransplantEvent;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.ri.kafka.KafkaJsonEventSubscriber.NessieEventsJsonSerializer;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.ImmutableEventSubscription;
import org.projectnessie.events.spi.ImmutableEventSystemConfiguration;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;

public class TestKafkaJsonEventSubscriber {

  UUID id = UUID.randomUUID();

  Instant now = Instant.now();

  Reference branch1 = Branch.of("branch1", "cafebabe");
  Reference branch2 = Branch.of("branch2", "deadbeef");

  @Test
  public void testCommit() {

    CommitEvent upstreamEvent =
        ImmutableCommitEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .reference(branch1)
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(now)
                    .authorTime(now)
                    .addAllAuthors("author")
                    .message("message")
                    .build())
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch1");
    assertThat(actual.value()).asInstanceOf(type(CommitEvent.class)).isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.COMMIT);
  }

  @Test
  void testMerge() {

    MergeEvent upstreamEvent =
        ImmutableMergeEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .sourceHash("sourceHash")
            .commonAncestorHash("commonAncestorHash")
            .sourceReference(branch1)
            .targetReference(branch2)
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch2");
    assertThat(actual.value()).asInstanceOf(type(MergeEvent.class)).isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.MERGE);
  }

  @Test
  void testTransplant() {

    TransplantEvent upstreamEvent =
        ImmutableTransplantEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .targetReference(branch2)
            .commitCount(3)
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch2");
    assertThat(actual.value()).asInstanceOf(type(TransplantEvent.class)).isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.TRANSPLANT);
  }

  @Test
  public void testContentStored() {

    ContentStoredEvent upstreamEvent =
        ImmutableContentStoredEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .commitCreationTimestamp(now)
            .hash("hash")
            .reference(branch1)
            .contentKey(ContentKey.of("folder1", "folder2", "table1"))
            .content(IcebergTable.of("metadataLocation", 1L, 2, 3, 4, "id"))
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch1");
    assertThat(actual.value())
        .asInstanceOf(type(ContentStoredEvent.class))
        .isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.CONTENT_STORED);
  }

  @Test
  public void testContentRemoved() {

    ContentRemovedEvent upstreamEvent =
        ImmutableContentRemovedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .commitCreationTimestamp(now)
            .hash("hash")
            .reference(branch1)
            .contentKey(ContentKey.of("folder1", "folder2", "table1"))
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch1");
    assertThat(actual.value())
        .asInstanceOf(type(ContentRemovedEvent.class))
        .isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.CONTENT_REMOVED);
  }

  @Test
  public void testReferenceCreated() {

    ReferenceCreatedEvent upstreamEvent =
        ImmutableReferenceCreatedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch1)
            .hashAfter("hashAfter")
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch1");
    assertThat(actual.value())
        .asInstanceOf(type(ReferenceCreatedEvent.class))
        .isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.REFERENCE_CREATED);
  }

  @Test
  public void testReferenceDeleted() {

    ReferenceDeletedEvent upstreamEvent =
        ImmutableReferenceDeletedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch1)
            .hashBefore("hashBefore")
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch1");
    assertThat(actual.value())
        .asInstanceOf(type(ReferenceDeletedEvent.class))
        .isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.REFERENCE_DELETED);
  }

  @Test
  public void testReferenceUpdated() {

    ReferenceUpdatedEvent upstreamEvent =
        ImmutableReferenceUpdatedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch1)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .build();

    ProducerRecord<String, Event> actual = sendAndReceive(upstreamEvent);
    assertThat(actual.key()).isEqualTo("repo1:branch1");
    assertThat(actual.value())
        .asInstanceOf(type(ReferenceUpdatedEvent.class))
        .isEqualTo(upstreamEvent);
    assertCommonHeaders(actual, EventType.REFERENCE_UPDATED);
  }

  protected ProducerRecord<String, Event> sendAndReceive(Event event) {
    try (MockProducer<String, Event> mockProducer = mockProducer();
        KafkaJsonEventSubscriber subscriber =
            new KafkaJsonEventSubscriber(new Properties(), p -> mockProducer)) {
      subscriber.onSubscribe(createSubscription());
      subscriber.onEvent(event);
      return mockProducer.history().stream().findFirst().orElseThrow();
    }
  }

  protected void assertCommonHeaders(ProducerRecord<String, Event> actual, EventType eventType) {
    assertThat(actual.headers())
        .extracting(Header::key, h -> new String(h.value(), StandardCharsets.UTF_8))
        .contains(
            tuple(EVENT_TYPE.getKey(), eventType.name()),
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "alice"),
            tuple(EVENT_CREATION_TIME.getKey(), now.toString()));
    if (eventType == EventType.COMMIT
        || eventType == EventType.CONTENT_REMOVED
        || eventType == EventType.CONTENT_STORED) {
      assertThat(actual.headers().lastHeader(COMMIT_CREATION_TIME.getKey()).value())
          .isEqualTo(now.toString().getBytes(StandardCharsets.UTF_8));
    }
  }

  private static MockProducer<String, Event> mockProducer() {
    Serializer<String> keySerializer = new StringSerializer();
    keySerializer.configure(new HashMap<>(), true);
    Serializer<Event> valueSerializer = new NessieEventsJsonSerializer();
    valueSerializer.configure(Map.of(), false);
    return new MockProducer<>(true, keySerializer, valueSerializer);
  }

  private static EventSubscription createSubscription() {
    return ImmutableEventSubscription.builder()
        .id(UUID.randomUUID())
        .systemConfiguration(
            ImmutableEventSystemConfiguration.builder()
                .specVersion("2.0.0")
                .minSupportedApiVersion(1)
                .maxSupportedApiVersion(2)
                .build())
        .build();
  }
}

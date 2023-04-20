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
import static org.projectnessie.events.ri.kafka.KafkaEventSubscriber.Header.COMMIT_CREATION_TIME;
import static org.projectnessie.events.ri.kafka.KafkaEventSubscriber.Header.EVENT_CREATION_TIME;
import static org.projectnessie.events.ri.kafka.KafkaEventSubscriber.Header.EVENT_TYPE;
import static org.projectnessie.events.ri.kafka.KafkaEventSubscriber.Header.INITIATOR;
import static org.projectnessie.events.ri.kafka.KafkaEventSubscriber.Header.REPOSITORY_ID;
import static org.projectnessie.events.ri.kafka.KafkaEventSubscriber.Header.SPEC_VERSION;

import com.example.nessie.events.generated.CommitEvent;
import com.example.nessie.events.generated.OperationEvent;
import com.example.nessie.events.generated.OperationEventType;
import com.example.nessie.events.generated.ReferenceEvent;
import com.example.nessie.events.generated.ReferenceEventType;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
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
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableCommitMeta;
import org.projectnessie.events.api.ImmutableContent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableReference;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.ImmutableReferenceUpdatedEvent;
import org.projectnessie.events.api.Reference;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.ImmutableEventSubscription;
import org.projectnessie.events.spi.ImmutableEventSystemConfiguration;

public class TestKafkaEventSubscriber {

  UUID id = UUID.randomUUID();

  Instant now = Instant.now();

  Reference branch =
      ImmutableReference.builder()
          .simpleName("branch1")
          .fullName("refs/heads/branch1")
          .type("BRANCH")
          .build();

  @Test
  public void testCommit() {

    ImmutableCommitEvent upstreamEvent =
        ImmutableCommitEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .reference(branch)
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTimestamp(now)
                    .authorTimestamp(now)
                    .addAuthor("author")
                    .message("message")
                    .build())
            .build();

    ProducerRecord<String, Object> actual = sendAndReceive(upstreamEvent);

    assertThat(actual.key()).isEqualTo("repo1:refs/heads/branch1");

    assertThat(actual.value())
        .asInstanceOf(type(CommitEvent.class))
        .extracting(
            CommitEvent::getId,
            CommitEvent::getHashBefore,
            CommitEvent::getHashAfter,
            CommitEvent::getReference)
        .containsExactly(id, "hashBefore", "hashAfter", "refs/heads/branch1");

    assertCommonHeaders(actual, EventType.COMMIT);
  }

  @Test
  public void testContentStored() {

    ImmutableContentStoredEvent upstreamEvent =
        ImmutableContentStoredEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .commitCreationTimestamp(now)
            .hash("hash")
            .reference(branch)
            .contentKey(ContentKey.of("folder1", "folder2", "table1"))
            .content(
                ImmutableContent.builder()
                    .id("id")
                    .type("ICEBERG_TABLE")
                    .properties(Map.of("key", "value"))
                    .build())
            .build();

    ProducerRecord<String, Object> actual = sendAndReceive(upstreamEvent);

    assertThat(actual.key()).isEqualTo("repo1:refs/heads/branch1");

    assertThat(actual.value())
        .asInstanceOf(type(OperationEvent.class))
        .extracting(
            OperationEvent::getType,
            OperationEvent::getId,
            OperationEvent::getHash,
            OperationEvent::getReference,
            OperationEvent::getContentKey,
            OperationEvent::getContentType,
            OperationEvent::getContentId,
            OperationEvent::getContentProperties)
        .containsExactly(
            OperationEventType.PUT,
            id,
            "hash",
            "refs/heads/branch1",
            "folder1.folder2.table1",
            "ICEBERG_TABLE",
            "id",
            Map.of("key", "value"));

    assertCommonHeaders(actual, EventType.CONTENT_STORED);
  }

  @Test
  public void testContentRemoved() {

    ImmutableContentRemovedEvent upstreamEvent =
        ImmutableContentRemovedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .commitCreationTimestamp(now)
            .hash("hash")
            .reference(branch)
            .contentKey(ContentKey.of("folder1", "folder2", "table1"))
            .build();

    ProducerRecord<String, Object> actual = sendAndReceive(upstreamEvent);

    assertThat(actual.key()).isEqualTo("repo1:refs/heads/branch1");

    assertThat(actual.value())
        .asInstanceOf(type(OperationEvent.class))
        .extracting(
            OperationEvent::getType,
            OperationEvent::getId,
            OperationEvent::getHash,
            OperationEvent::getReference,
            OperationEvent::getContentKey,
            OperationEvent::getContentType,
            OperationEvent::getContentId,
            OperationEvent::getContentProperties)
        .containsExactly(
            OperationEventType.DELETE,
            id,
            "hash",
            "refs/heads/branch1",
            "folder1.folder2.table1",
            null,
            null,
            Map.of());

    assertCommonHeaders(actual, EventType.CONTENT_REMOVED);
  }

  @Test
  public void testReferenceCreated() {

    ImmutableReferenceCreatedEvent upstreamEvent =
        ImmutableReferenceCreatedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch)
            .hashAfter("hashAfter")
            .build();

    ProducerRecord<String, Object> actual = sendAndReceive(upstreamEvent);

    assertThat(actual.key()).isEqualTo("repo1:refs/heads/branch1");

    assertThat(actual.value())
        .asInstanceOf(type(ReferenceEvent.class))
        .extracting(
            ReferenceEvent::getId,
            ReferenceEvent::getType,
            ReferenceEvent::getHashBefore,
            ReferenceEvent::getHashAfter,
            ReferenceEvent::getReference)
        .containsExactly(id, ReferenceEventType.CREATED, null, "hashAfter", "refs/heads/branch1");

    assertCommonHeaders(actual, EventType.REFERENCE_CREATED);
  }

  @Test
  public void testReferenceDeleted() {

    ImmutableReferenceDeletedEvent upstreamEvent =
        ImmutableReferenceDeletedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch)
            .hashBefore("hashBefore")
            .build();

    ProducerRecord<String, Object> actual = sendAndReceive(upstreamEvent);

    assertThat(actual.key()).isEqualTo("repo1:refs/heads/branch1");

    assertThat(actual.value())
        .asInstanceOf(type(ReferenceEvent.class))
        .extracting(
            ReferenceEvent::getId,
            ReferenceEvent::getType,
            ReferenceEvent::getHashBefore,
            ReferenceEvent::getHashAfter,
            ReferenceEvent::getReference)
        .containsExactly(id, ReferenceEventType.DELETED, "hashBefore", null, "refs/heads/branch1");

    assertCommonHeaders(actual, EventType.REFERENCE_DELETED);
  }

  @Test
  public void testReferenceUpdated() {

    ImmutableReferenceUpdatedEvent upstreamEvent =
        ImmutableReferenceUpdatedEvent.builder()
            .id(id)
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .build();

    ProducerRecord<String, Object> actual = sendAndReceive(upstreamEvent);

    assertThat(actual.key()).isEqualTo("repo1:refs/heads/branch1");

    assertThat(actual.value())
        .asInstanceOf(type(ReferenceEvent.class))
        .extracting(
            ReferenceEvent::getId,
            ReferenceEvent::getType,
            ReferenceEvent::getHashBefore,
            ReferenceEvent::getHashAfter,
            ReferenceEvent::getReference)
        .containsExactly(
            id, ReferenceEventType.REASSIGNED, "hashBefore", "hashAfter", "refs/heads/branch1");

    assertCommonHeaders(actual, EventType.REFERENCE_UPDATED);
  }

  protected ProducerRecord<String, Object> sendAndReceive(Event event) {
    try (MockProducer<String, Object> mockProducer = mockProducer();
        KafkaEventSubscriber subscriber =
            new KafkaEventSubscriber(new Properties(), p -> mockProducer)) {
      subscriber.onSubscribe(createSubscription());
      subscriber.onEvent(event);
      return mockProducer.history().stream().findFirst().orElseThrow();
    }
  }

  protected void assertCommonHeaders(ProducerRecord<String, Object> actual, EventType eventType) {
    assertThat(actual.headers())
        .extracting(Header::key, h -> new String(h.value(), StandardCharsets.UTF_8))
        .contains(
            tuple(EVENT_TYPE.getKey(), eventType.name()),
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
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

  private static MockProducer<String, Object> mockProducer() {
    Serializer<String> keySerializer = new StringSerializer();
    keySerializer.configure(new HashMap<>(), true);
    Serializer<Object> valueSerializer = new KafkaAvroSerializer();
    valueSerializer.configure(Map.of("schema.registry.url", "mock://schema-registry"), false);
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

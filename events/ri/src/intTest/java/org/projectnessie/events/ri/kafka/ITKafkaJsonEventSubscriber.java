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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.COMMIT_CREATION_TIME;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.EVENT_CREATION_TIME;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.EVENT_TYPE;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.INITIATOR;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.MAX_API_VERSION;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.REPOSITORY_ID;
import static org.projectnessie.events.ri.kafka.AbstractKafkaEventSubscriber.Header.SPEC_VERSION;

import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.ri.kafka.KafkaJsonEventSubscriber.NessieEventsJsonDeserializer;
import org.projectnessie.events.ri.kafka.KafkaJsonEventSubscriber.NessieEventsJsonSerializer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ITKafkaJsonEventSubscriber extends AbstractKafkaEventSubscriberTests {

  @Override
  protected Class<?> subscriberClass() {
    return KafkaJsonEventSubscriber.class;
  }

  @Override
  protected Class<?> serializerClass() {
    return NessieEventsJsonSerializer.class;
  }

  @Override
  protected Class<?> deserializerClass() {
    return NessieEventsJsonDeserializer.class;
  }

  @Override
  protected int expectedNumEvents() {
    return 11;
  }

  @Override
  protected void assertEventsReceived(
      KafkaConsumer<String, Object> consumer, List<ConsumerRecord<String, Object>> records) {
    assertThat(records).hasSize(12);
    assertMainCreated(records.remove(0));
    assertCommitAlice(records.remove(0));
    assertTable1Created(records.remove(0));
    assertDevCreated(records.remove(0));
    assertCommitBob(records.remove(0));
    assertTable2Created(records.remove(0));
    assertCommitCharlie(records.remove(0));
    assertTable1DeletedOnDev(records.remove(0));
    assertMergeDave(records.remove(0));
    assertCommitDave(records.remove(0));
    assertTable1DeletedOnMain(records.remove(0));
    assertDevDeleted(records.remove(0));
  }

  private void assertMainCreated(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "admin"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(1).toString()),
            tuple(EVENT_TYPE.getKey(), "REFERENCE_CREATED"));
    assertThat(record.value()).isInstanceOf(ReferenceCreatedEvent.class);
    ReferenceCreatedEvent event = (ReferenceCreatedEvent) record.value();
    assertThat(event).isEqualTo(mainCreatedEvent);
  }

  private void assertCommitAlice(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "alice"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(2).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(2).toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event).isEqualTo(aliceCommit);
  }

  private void assertTable1Created(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "alice"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(2).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(2).toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_STORED"));
    assertThat(record.value()).isInstanceOf(ContentStoredEvent.class);
    ContentStoredEvent event = (ContentStoredEvent) record.value();
    assertThat(event).isEqualTo(table1Created);
  }

  private void assertDevCreated(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:dev");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "admin"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(3).toString()),
            tuple(EVENT_TYPE.getKey(), "REFERENCE_CREATED"));
    assertThat(record.value()).isInstanceOf(ReferenceCreatedEvent.class);
    ReferenceCreatedEvent event = (ReferenceCreatedEvent) record.value();
    assertThat(event).isEqualTo(devCreatedEvent);
  }

  private void assertCommitBob(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "bob"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(4).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(4).toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event).isEqualTo(bobCommit);
  }

  private void assertTable2Created(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "bob"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(4).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(4).toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_STORED"));
    assertThat(record.value()).isInstanceOf(ContentStoredEvent.class);
    ContentStoredEvent event = (ContentStoredEvent) record.value();
    assertThat(event).isEqualTo(table2Created);
  }

  private void assertCommitCharlie(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:dev");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "charlie"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(5).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(5).toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event).isEqualTo(charlieCommit);
  }

  private void assertTable1DeletedOnDev(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:dev");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "charlie"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(5).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(5).toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_REMOVED"));
    assertThat(record.value()).isInstanceOf(ContentRemovedEvent.class);
    ContentRemovedEvent event = (ContentRemovedEvent) record.value();
    assertThat(event).isEqualTo(table1DeletedOnDev);
  }

  private void assertMergeDave(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "dave"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(6).toString()),
            tuple(EVENT_TYPE.getKey(), "MERGE"));
    assertThat(record.value()).isInstanceOf(MergeEvent.class);
    MergeEvent event = (MergeEvent) record.value();
    assertThat(event).isEqualTo(daveMerge);
  }

  private void assertCommitDave(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "dave"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(6).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(6).toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event).isEqualTo(daveCommit);
  }

  private void assertTable1DeletedOnMain(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:main");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "dave"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(6).toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), Instant.ofEpochSecond(6).toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_REMOVED"));
    assertThat(record.value()).isInstanceOf(ContentRemovedEvent.class);
    ContentRemovedEvent event = (ContentRemovedEvent) record.value();
    assertThat(event).isEqualTo(table1DeletedOnMain);
  }

  private void assertDevDeleted(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:dev");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(MAX_API_VERSION.getKey(), "\u0002"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "admin"),
            tuple(EVENT_CREATION_TIME.getKey(), Instant.ofEpochSecond(7).toString()),
            tuple(EVENT_TYPE.getKey(), "REFERENCE_DELETED"));
    assertThat(record.value()).isInstanceOf(ReferenceDeletedEvent.class);
    ReferenceDeletedEvent event = (ReferenceDeletedEvent) record.value();
    assertThat(event).isEqualTo(devDeletedEvent);
  }
}

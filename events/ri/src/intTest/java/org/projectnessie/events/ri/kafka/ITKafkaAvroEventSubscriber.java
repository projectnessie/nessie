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

import com.example.nessie.events.generated.CommitEvent;
import com.example.nessie.events.generated.OperationEvent;
import com.example.nessie.events.generated.OperationEventType;
import com.example.nessie.events.generated.ReferenceEvent;
import com.example.nessie.events.generated.ReferenceEventType;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ITKafkaAvroEventSubscriber extends AbstractKafkaEventSubscriberTests {

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> SCHEMA_REGISTRY =
      new GenericContainer<>(
              ContainerSpecHelper.builder()
                  .name("schema-registry")
                  .containerClass(ITKafkaAvroEventSubscriber.class)
                  .build()
                  .dockerImageName(null))
          .withNetwork(NETWORK)
          .dependsOn(KAFKA)
          .withExposedPorts(8081)
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9092");

  @Override
  protected Class<?> subscriberClass() {
    return KafkaAvroEventSubscriber.class;
  }

  @Override
  protected Class<?> serializerClass() {
    return KafkaAvroSerializer.class;
  }

  @Override
  protected Class<?> deserializerClass() {
    return KafkaAvroDeserializer.class;
  }

  @Override
  protected int expectedNumEvents() {
    return 8;
  }

  @Override
  protected Properties generateConsumerConfig() {
    Properties props = super.generateConsumerConfig();
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    return props;
  }

  @Override
  protected void addCommonConfig(Properties props) {
    super.addCommonConfig(props);
    props.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:" + SCHEMA_REGISTRY.getMappedPort(8081));
    props.put(
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
  }

  @Override
  protected void assertEventsReceived(
      KafkaConsumer<String, Object> consumer, List<ConsumerRecord<String, Object>> records) {
    assertThat(records).hasSize(11);
    assertMainCreated(records.remove(0));
    assertCommitAlice(records.remove(0));
    assertTable1Created(records.remove(0));
    assertDevCreated(records.remove(0));
    assertCommitBob(records.remove(0));
    assertTable2Created(records.remove(0));
    assertCommitCharlie(records.remove(0));
    assertTable1DeletedOnDev(records.remove(0));
    // no merge event since the subscriber filtered it out
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
    assertThat(record.value()).isInstanceOf(ReferenceEvent.class);
    ReferenceEvent event = (ReferenceEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getReference()).isEqualTo("main");
    assertThat(event.getHashBefore()).isNull();
    assertThat(event.getHashAfter()).isEqualTo("00000000");
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
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("00000000");
    assertThat(event.getHashAfter()).isEqualTo("11111111");
    assertThat(event.getReference()).isEqualTo("main");
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
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.PUT);
    assertThat(event.getHash()).isEqualTo("11111111");
    assertThat(event.getReference()).isEqualTo("main");
    assertThat(event.getContentKey()).isEqualTo("directory.subdirectory.table1");
    assertThat(event.getContentId()).isEqualTo("id1");
    assertThat(event.getContentType()).isEqualTo("ICEBERG_TABLE");
    assertThat(event.getContentProperties())
        .isEqualTo(
            Map.of(
                "metadataLocation",
                "location1",
                "snapshotId",
                "1",
                "schemaId",
                "1",
                "specId",
                "1",
                "sortOrderId",
                "1"));
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
    assertThat(record.value()).isInstanceOf(ReferenceEvent.class);
    ReferenceEvent event = (ReferenceEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getReference()).isEqualTo("dev");
    assertThat(event.getHashBefore()).isNull();
    assertThat(event.getHashAfter()).isEqualTo("11111111");
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
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("11111111");
    assertThat(event.getHashAfter()).isEqualTo("22222222");
    assertThat(event.getReference()).isEqualTo("main");
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
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.PUT);
    assertThat(event.getHash()).isEqualTo("22222222");
    assertThat(event.getReference()).isEqualTo("main");
    assertThat(event.getContentKey()).isEqualTo("directory.subdirectory.table2");
    assertThat(event.getContentId()).isEqualTo("id2");
    assertThat(event.getContentType()).isEqualTo("ICEBERG_TABLE");
    assertThat(event.getContentProperties())
        .isEqualTo(
            Map.of(
                "metadataLocation",
                "location2",
                "snapshotId",
                "2",
                "schemaId",
                "2",
                "specId",
                "2",
                "sortOrderId",
                "2"));
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
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("11111111");
    assertThat(event.getHashAfter()).isEqualTo("33333333");
    assertThat(event.getReference()).isEqualTo("dev");
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
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.DELETE);
    assertThat(event.getHash()).isEqualTo("33333333");
    assertThat(event.getReference()).isEqualTo("dev");
    assertThat(event.getContentKey()).isEqualTo("directory.subdirectory.table1");
    assertThat(event.getContentId()).isNull();
    assertThat(event.getContentType()).isNull();
    assertThat(event.getContentProperties()).isEmpty();
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
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("22222222");
    assertThat(event.getHashAfter()).isEqualTo("44444444");
    assertThat(event.getReference()).isEqualTo("main");
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
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.DELETE);
    assertThat(event.getHash()).isEqualTo("44444444");
    assertThat(event.getReference()).isEqualTo("main");
    assertThat(event.getContentKey()).isEqualTo("directory.subdirectory.table1");
    assertThat(event.getContentId()).isNull();
    assertThat(event.getContentType()).isNull();
    assertThat(event.getContentProperties()).isEmpty();
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
    assertThat(record.value()).isInstanceOf(ReferenceEvent.class);
    ReferenceEvent event = (ReferenceEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(ReferenceEventType.DELETED);
    assertThat(event.getHashBefore()).isEqualTo("33333333");
    assertThat(event.getHashAfter()).isNull();
  }
}

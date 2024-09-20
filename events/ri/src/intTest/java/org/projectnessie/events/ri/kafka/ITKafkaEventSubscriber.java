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
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.ImmutableEventSubscription;
import org.projectnessie.events.spi.ImmutableEventSystemConfiguration;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ITKafkaEventSubscriber {

  private static final Network NETWORK = Network.newNetwork();

  @Container
  private static final KafkaContainer KAFKA =
      new KafkaContainer(
              ContainerSpecHelper.builder()
                  .name("kafka")
                  .containerClass(ITKafkaEventSubscriber.class)
                  .build()
                  .dockerImageName(null))
          .withNetwork(NETWORK)
          .withNetworkAliases("broker");

  @SuppressWarnings("resource")
  @Container
  private static final GenericContainer<?> SCHEMA_REGISTRY =
      new GenericContainer<>(
              ContainerSpecHelper.builder()
                  .name("schema-registry")
                  .containerClass(ITKafkaEventSubscriber.class)
                  .build()
                  .dockerImageName(null))
          .withNetwork(NETWORK)
          .dependsOn(KAFKA)
          .withExposedPorts(8081)
          .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry")
          .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9092");

  private static final String TOPIC_NAME = "nessie.events";

  private static final Reference BRANCH = Branch.of("branch1", "cafebabe");

  private static Properties consumerConfig;

  @BeforeAll
  static void setUpConfig() throws IOException {
    consumerConfig = generateConsumerConfig();
    Properties producerConfig = generateProducerConfig();
    Path file = writeConfig(producerConfig);
    System.setProperty(
        KafkaEventSubscriber.NESSIE_KAFKA_PROPERTIES_FILE_SYS_PROP,
        file.toAbsolutePath().toString());
  }

  @Test
  public void testProduceConsume() {
    try (KafkaEventSubscriber producer = loadKafkaEventSubscriberSpi();
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerConfig)) {

      producer.onSubscribe(createSubscription());
      consumer.subscribe(List.of(TOPIC_NAME));

      emitEvents(producer);
      assertEventsReceived(consumer);
    }
  }

  private static Properties generateProducerConfig() {
    Properties props = new Properties();
    props.put(KafkaEventSubscriber.ConfigOption.TOPIC_NAME.getKey(), TOPIC_NAME);
    props.put(KafkaEventSubscriber.ConfigOption.REPOSITORY_IDS.getKey(), "repo1, repo2");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    addCommonConfig(props);
    return props;
  }

  private static Properties generateConsumerConfig() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
    addCommonConfig(props);
    return props;
  }

  private static void addCommonConfig(Properties props) {
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:" + SCHEMA_REGISTRY.getMappedPort(8081));
    props.put(
        AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class);
  }

  private static Path writeConfig(Properties props) throws IOException {
    Path file = Files.createTempFile("nessie-kafka-test", ".properties");
    Properties toStore = new Properties();
    props.forEach(
        (k, v) -> toStore.put(k, v instanceof Class ? ((Class<?>) v).getName() : v.toString()));
    toStore.store(new FileWriter(file.toFile(), UTF_8), null);
    return file;
  }

  private static KafkaEventSubscriber loadKafkaEventSubscriberSpi() {
    return (KafkaEventSubscriber)
        ServiceLoader.load(EventSubscriber.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(s -> s instanceof KafkaEventSubscriber)
            .findFirst()
            .orElseThrow();
  }

  private EventSubscription createSubscription() {
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

  private Instant i1;
  private Instant i2;
  private Instant i3;
  private Instant i4;
  private Instant i5;

  private void emitEvents(KafkaEventSubscriber subscriber) {

    // Ten seconds ago...
    i1 = Instant.now().minusSeconds(10);

    // User admin creates branch1 => REFERENCE_CREATED event
    subscriber.onEvent(
        ImmutableReferenceCreatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("admin")
            .eventCreationTimestamp(i1)
            .reference(BRANCH)
            .hashAfter("hash1")
            .build());

    // User alice commits a new table to branch1 => COMMIT + CONTENT_STORED events
    i2 = i1.plusSeconds(1);
    subscriber.onEvent(
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(i2)
            .hashBefore("hash1")
            .hashAfter("hash2")
            .reference(BRANCH)
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i2)
                    .authorTime(i2)
                    .addAllAuthors("alice")
                    .message("commit1")
                    .build())
            .build());
    subscriber.onEvent(
        ImmutableContentStoredEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(i2)
            .commitCreationTimestamp(i2)
            .hash("hash2")
            .reference(BRANCH)
            .contentKey(ContentKey.of("directory", "subdirectory", "table1"))
            .content(IcebergTable.of("location1", 1L, 1, 1, 1, "id1"))
            .build());

    // User bob updates the table => COMMIT + CONTENT_STORED events
    i3 = i2.plusSeconds(1);
    subscriber.onEvent(
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("bob")
            .eventCreationTimestamp(i3)
            .hashBefore("hash2")
            .hashAfter("hash3")
            .reference(BRANCH)
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i3)
                    .authorTime(i3)
                    .addAllAuthors("bob")
                    .message("commit2")
                    .build())
            .build());
    subscriber.onEvent(
        ImmutableContentStoredEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("bob")
            .eventCreationTimestamp(i3)
            .commitCreationTimestamp(i3)
            .hash("hash3")
            .reference(BRANCH)
            .contentKey(ContentKey.of("directory", "subdirectory", "table1"))
            .content(IcebergTable.of("location2", 2L, 2, 2, 2, "id2"))
            .build());

    // User charlie deletes the table => COMMIT + CONTENT_REMOVED events
    i4 = i3.plusSeconds(1);
    subscriber.onEvent(
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("charlie")
            .eventCreationTimestamp(i4)
            .hashBefore("hash3")
            .hashAfter("hash4")
            .reference(BRANCH)
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i4)
                    .authorTime(i4)
                    .addAllAuthors("charlie")
                    .message("commit3")
                    .build())
            .build());
    subscriber.onEvent(
        ImmutableContentRemovedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("charlie")
            .eventCreationTimestamp(i4)
            .commitCreationTimestamp(i4)
            .hash("hash4")
            .reference(BRANCH)
            .contentKey(ContentKey.of("directory", "subdirectory", "table1"))
            .build());

    // User admin deletes branch1 => REFERENCE_DELETED event
    i5 = i4.plusSeconds(1);
    subscriber.onEvent(
        ImmutableReferenceDeletedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("admin")
            .eventCreationTimestamp(i5)
            .reference(BRANCH)
            .hashBefore("hash4")
            .build());
  }

  private void assertEventsReceived(KafkaConsumer<String, Object> consumer) {
    List<ConsumerRecord<String, Object>> records = new ArrayList<>();
    Instant start = Instant.now();
    Duration elapsed;
    do {
      consumer.poll(Duration.ofMillis(100)).forEach(records::add);
      elapsed = Duration.between(start, Instant.now());
    } while (elapsed.compareTo(Duration.ofSeconds(5)) < 0 && records.size() < 8);
    assertThat(records).hasSize(8);
    assertReferenceCreated(records.get(0));
    assertCommitAlice(records.get(1));
    assertTableCreated(records.get(2));
    assertCommitBob(records.get(3));
    assertTableUpdated(records.get(4));
    assertCommitCharlie(records.get(5));
    assertTableDeleted(records.get(6));
    assertReferenceDeleted(records.get(7));
  }

  private void assertReferenceCreated(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "admin"),
            tuple(EVENT_CREATION_TIME.getKey(), i1.toString()),
            tuple(EVENT_TYPE.getKey(), "REFERENCE_CREATED"));
    assertThat(record.value()).isInstanceOf(ReferenceEvent.class);
    ReferenceEvent event = (ReferenceEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getReference()).isEqualTo("branch1");
    assertThat(event.getHashBefore()).isNull();
    assertThat(event.getHashAfter()).isEqualTo("hash1");
  }

  private void assertCommitAlice(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "alice"),
            tuple(EVENT_CREATION_TIME.getKey(), i2.toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), i2.toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("hash1");
    assertThat(event.getHashAfter()).isEqualTo("hash2");
    assertThat(event.getReference()).isEqualTo("branch1");
  }

  private void assertTableCreated(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "alice"),
            tuple(EVENT_CREATION_TIME.getKey(), i2.toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), i2.toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_STORED"));
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.PUT);
    assertThat(event.getHash()).isEqualTo("hash2");
    assertThat(event.getReference()).isEqualTo("branch1");
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

  private void assertCommitBob(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "bob"),
            tuple(EVENT_CREATION_TIME.getKey(), i3.toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), i3.toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("hash2");
    assertThat(event.getHashAfter()).isEqualTo("hash3");
    assertThat(event.getReference()).isEqualTo("branch1");
  }

  private void assertTableUpdated(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "bob"),
            tuple(EVENT_CREATION_TIME.getKey(), i3.toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), i3.toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_STORED"));
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.PUT);
    assertThat(event.getHash()).isEqualTo("hash3");
    assertThat(event.getReference()).isEqualTo("branch1");
    assertThat(event.getContentKey()).isEqualTo("directory.subdirectory.table1");
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
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "charlie"),
            tuple(EVENT_CREATION_TIME.getKey(), i4.toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), i4.toString()),
            tuple(EVENT_TYPE.getKey(), "COMMIT"));
    assertThat(record.value()).isInstanceOf(CommitEvent.class);
    CommitEvent event = (CommitEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getHashBefore()).isEqualTo("hash3");
    assertThat(event.getHashAfter()).isEqualTo("hash4");
    assertThat(event.getReference()).isEqualTo("branch1");
  }

  private void assertTableDeleted(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "charlie"),
            tuple(EVENT_CREATION_TIME.getKey(), i4.toString()),
            tuple(COMMIT_CREATION_TIME.getKey(), i4.toString()),
            tuple(EVENT_TYPE.getKey(), "CONTENT_REMOVED"));
    assertThat(record.value()).isInstanceOf(OperationEvent.class);
    OperationEvent event = (OperationEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(OperationEventType.DELETE);
    assertThat(event.getHash()).isEqualTo("hash4");
    assertThat(event.getReference()).isEqualTo("branch1");
    assertThat(event.getContentKey()).isEqualTo("directory.subdirectory.table1");
    assertThat(event.getContentId()).isNull();
    assertThat(event.getContentType()).isNull();
    assertThat(event.getContentProperties()).isEmpty();
  }

  private void assertReferenceDeleted(ConsumerRecord<String, Object> record) {
    assertThat(record.key()).isEqualTo("repo1:branch1");
    assertThat(record.headers())
        .extracting(Header::key, header -> new String(header.value(), UTF_8))
        .containsExactlyInAnyOrder(
            tuple(SPEC_VERSION.getKey(), "2.0.0"),
            tuple(REPOSITORY_ID.getKey(), "repo1"),
            tuple(INITIATOR.getKey(), "admin"),
            tuple(EVENT_CREATION_TIME.getKey(), i5.toString()),
            tuple(EVENT_TYPE.getKey(), "REFERENCE_DELETED"));
    assertThat(record.value()).isInstanceOf(ReferenceEvent.class);
    ReferenceEvent event = (ReferenceEvent) record.value();
    assertThat(event.getId()).isNotNull();
    assertThat(event.getType()).isEqualTo(ReferenceEventType.DELETED);
    assertThat(event.getHashBefore()).isEqualTo("hash4");
    assertThat(event.getHashAfter()).isNull();
  }
}

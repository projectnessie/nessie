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

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.UUID;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableMergeEvent;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
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
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;

@TestInstance(Lifecycle.PER_CLASS)
abstract class AbstractKafkaEventSubscriberTests {

  protected static final Network NETWORK = Network.newNetwork();

  @Container
  protected static final ConfluentKafkaContainer KAFKA =
      new ConfluentKafkaContainer(
              ContainerSpecHelper.builder()
                  .name("kafka")
                  .containerClass(ITKafkaAvroEventSubscriber.class)
                  .build()
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
          .withNetwork(NETWORK)
          .withNetworkAliases("broker");

  private static final String TOPIC_NAME = "nessie.events";

  private Properties consumerConfig;

  @BeforeAll
  void setUpConfig() throws IOException {
    consumerConfig = generateConsumerConfig();
    Properties producerConfig = generateProducerConfig();
    Path file = writeConfig(producerConfig);
    System.setProperty(
        AbstractKafkaEventSubscriber.NESSIE_KAFKA_PROPERTIES_FILE_SYS_PROP,
        file.toAbsolutePath().toString());
  }

  @Test
  public void testProduceConsume() throws Exception {
    try (EventSubscriber producer = loadKafkaEventSubscriberSpi(subscriberClass());
        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerConfig)) {

      producer.onSubscribe(createSubscription());
      consumer.subscribe(List.of(TOPIC_NAME));

      emitEvents(producer);
      List<ConsumerRecord<String, Object>> received = awaitEvents(consumer, expectedNumEvents());
      assertEventsReceived(consumer, received);
    }
  }

  protected abstract Class<?> subscriberClass();

  protected abstract Class<?> serializerClass();

  protected abstract Class<?> deserializerClass();

  protected abstract int expectedNumEvents();

  protected abstract void assertEventsReceived(
      KafkaConsumer<String, Object> consumer, List<ConsumerRecord<String, Object>> received);

  protected Properties generateProducerConfig() {
    Properties props = new Properties();
    props.put(AbstractKafkaEventSubscriber.ConfigOption.TOPIC_NAME.getKey(), TOPIC_NAME);
    props.put(AbstractKafkaEventSubscriber.ConfigOption.REPOSITORY_IDS.getKey(), "repo1, repo2");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializerClass());
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    addCommonConfig(props);
    return props;
  }

  protected Properties generateConsumerConfig() {
    Properties props = new Properties();
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerClass());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    addCommonConfig(props);
    return props;
  }

  protected void addCommonConfig(Properties props) {
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
  }

  private static Path writeConfig(Properties props) throws IOException {
    Path file = Files.createTempFile("nessie-kafka-test", ".properties");
    Properties toStore = new Properties();
    props.forEach(
        (k, v) -> toStore.put(k, v instanceof Class ? ((Class<?>) v).getName() : v.toString()));
    toStore.store(new FileWriter(file.toFile(), UTF_8), null);
    return file;
  }

  private EventSubscriber loadKafkaEventSubscriberSpi(Class<?> subscriberClass) {
    return ServiceLoader.load(EventSubscriber.class).stream()
        .map(ServiceLoader.Provider::get)
        .filter(s -> s.getClass().equals(subscriberClass))
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

  protected ReferenceCreatedEvent mainCreatedEvent;
  protected CommitEvent aliceCommit;
  protected ContentStoredEvent table1Created;
  protected ReferenceCreatedEvent devCreatedEvent;
  protected CommitEvent bobCommit;
  protected ContentStoredEvent table2Created;
  protected CommitEvent charlieCommit;
  protected ContentRemovedEvent table1DeletedOnDev;
  protected MergeEvent daveMerge;
  protected CommitEvent daveCommit;
  protected ContentRemovedEvent table1DeletedOnMain;
  protected ReferenceDeletedEvent devDeletedEvent;

  private void emitEvents(EventSubscriber subscriber) {

    // Scenario:
    //
    //   main
    //    |
    // 11111111 (alice) => PUT table1
    //    |                                   dev
    //    +------------------------------------+
    //    |                                    |
    // 22222222 (bob) => PUT table2         33333333 (charlie) => DEL table1
    //    |                                    |
    //    +------------------------------------+
    //    |
    // 44444444 (dave) => MERGE dev into main
    //

    Instant i = Instant.ofEpochSecond(1);

    // User admin creates main => REFERENCE_CREATED event
    mainCreatedEvent =
        ImmutableReferenceCreatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("admin")
            .eventCreationTimestamp(i)
            .reference(Branch.of("main", "00000000"))
            .hashAfter(((Reference) Branch.of("main", "00000000")).getHash())
            .build();

    subscriber.onEvent(mainCreatedEvent);

    // User alice commits table1 to main => COMMIT + CONTENT_STORED events
    i = i.plusSeconds(1);

    aliceCommit =
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(i)
            .hashBefore("00000000")
            .hashAfter("11111111")
            .reference(Branch.of("main", "11111111"))
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i)
                    .authorTime(i)
                    .addAllAuthors("alice")
                    .message("create table1")
                    .build())
            .build();

    subscriber.onEvent(aliceCommit);

    table1Created =
        ImmutableContentStoredEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(i)
            .commitCreationTimestamp(i)
            .hash("11111111")
            .reference(Branch.of("main", "11111111"))
            .contentKey(ContentKey.of("directory", "subdirectory", "table1"))
            .content(IcebergTable.of("location1", 1L, 1, 1, 1, "id1"))
            .build();

    subscriber.onEvent(table1Created);

    // User admin creates dev from main => REFERENCE_CREATED event
    i = i.plusSeconds(1);

    devCreatedEvent =
        ImmutableReferenceCreatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("admin")
            .eventCreationTimestamp(i)
            .reference(Branch.of("dev", "11111111"))
            .hashAfter("11111111")
            .build();

    subscriber.onEvent(devCreatedEvent);

    // User bob creates table2 on main => COMMIT + CONTENT_STORED events
    i = i.plusSeconds(1);

    bobCommit =
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("bob")
            .eventCreationTimestamp(i)
            .hashBefore("11111111")
            .hashAfter("22222222")
            .reference(Branch.of("main", "22222222"))
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i)
                    .authorTime(i)
                    .addAllAuthors("bob")
                    .message("create table2")
                    .build())
            .build();

    subscriber.onEvent(bobCommit);

    table2Created =
        ImmutableContentStoredEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("bob")
            .eventCreationTimestamp(i)
            .commitCreationTimestamp(i)
            .hash("22222222")
            .reference(Branch.of("main", "22222222"))
            .contentKey(ContentKey.of("directory", "subdirectory", "table2"))
            .content(IcebergTable.of("location2", 2L, 2, 2, 2, "id2"))
            .build();

    subscriber.onEvent(table2Created);

    // User charlie deletes table1 on dev => COMMIT + CONTENT_REMOVED events
    i = i.plusSeconds(1);

    charlieCommit =
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("charlie")
            .eventCreationTimestamp(i)
            .hashBefore("11111111")
            .hashAfter("33333333")
            .reference(Branch.of("dev", "33333333"))
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i)
                    .authorTime(i)
                    .addAllAuthors("charlie")
                    .message("delete table1")
                    .build())
            .build();

    subscriber.onEvent(charlieCommit);

    table1DeletedOnDev =
        ImmutableContentRemovedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("charlie")
            .eventCreationTimestamp(i)
            .commitCreationTimestamp(i)
            .hash("33333333")
            .reference(Branch.of("dev", "33333333"))
            .contentKey(ContentKey.of("directory", "subdirectory", "table1"))
            .build();

    subscriber.onEvent(table1DeletedOnDev);

    // User dave merges dev into main => MERGE + COMMIT + CONTENT_REMOVED events
    i = i.plusSeconds(1);

    daveMerge =
        ImmutableMergeEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("dave")
            .eventCreationTimestamp(i)
            .hashBefore("22222222")
            .hashAfter("44444444")
            .sourceHash("33333333")
            .sourceReference(Branch.of("dev", "33333333"))
            .targetReference(Branch.of("main", "44444444"))
            .commonAncestorHash("11111111")
            .build();

    subscriber.onEvent(daveMerge);

    daveCommit =
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("dave")
            .eventCreationTimestamp(i)
            .hashBefore("22222222")
            .hashAfter("44444444")
            .reference(Branch.of("main", "44444444"))
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(i)
                    .authorTime(i)
                    .addAllAuthors("dave")
                    .message("merge dev into main")
                    .build())
            .build();

    subscriber.onEvent(daveCommit);

    table1DeletedOnMain =
        ImmutableContentRemovedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("dave")
            .eventCreationTimestamp(i)
            .commitCreationTimestamp(i)
            .hash("44444444")
            .reference(Branch.of("main", "44444444"))
            .contentKey(ContentKey.of("directory", "subdirectory", "table1"))
            .build();

    subscriber.onEvent(table1DeletedOnMain);

    // User admin deletes dev => REFERENCE_DELETED event
    i = i.plusSeconds(1);
    devDeletedEvent =
        ImmutableReferenceDeletedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("admin")
            .eventCreationTimestamp(i)
            .reference(Branch.of("dev", "33333333"))
            .hashBefore("33333333")
            .build();

    subscriber.onEvent(devDeletedEvent);
  }

  protected List<ConsumerRecord<String, Object>> awaitEvents(
      KafkaConsumer<String, Object> consumer, int numEvents) {
    List<ConsumerRecord<String, Object>> records = new ArrayList<>();
    Instant start = Instant.now();
    Duration elapsed;
    do {
      consumer.poll(Duration.ofMillis(100)).forEach(records::add);
      elapsed = Duration.between(start, Instant.now());
    } while (elapsed.compareTo(Duration.ofSeconds(5)) < 0 && records.size() < numEvents);
    return records;
  }
}

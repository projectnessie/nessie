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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MultiReferenceEvent;
import org.projectnessie.events.api.ReferenceEvent;
import org.projectnessie.events.spi.EventFilter;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link EventSubscriber} that publishes events to a Kafka topic.
 *
 * @param <T> The Kafka record value type emitted by this subscriber.
 */
public abstract class AbstractKafkaEventSubscriber<T> implements EventSubscriber {

  /** The environment variable pointing to the location of the configuration file to use. */
  public static final String NESSIE_KAFKA_PROPERTIES_FILE_ENV_VAR = "NESSIE_EVENTS_CONFIG_FILE";

  /**
   * The system property pointing to the location of the configuration file to use. If this is set,
   * it takes precedence over the environment variable {@value
   * #NESSIE_KAFKA_PROPERTIES_FILE_ENV_VAR}.
   */
  public static final String NESSIE_KAFKA_PROPERTIES_FILE_SYS_PROP = "nessie.events.config.file";

  /** The default location of the configuration file to use. */
  public static final String NESSIE_KAFKA_PROPERTIES_FILE_DEFAULT = "./nessie-kafka.properties";

  /** Configuration options for the subscriber. */
  public enum ConfigOption {
    TOPIC_NAME("nessie.events.topic", "nessie.events"),
    REPOSITORY_IDS("nessie.events.repository-ids", ""),
    ;

    private final String key;
    private final String defaultValue;

    ConfigOption(String key, String defaultValue) {
      this.key = key;
      this.defaultValue = defaultValue;
    }

    public String getKey() {
      return key;
    }

    public String getValue(Properties props) {
      String value = props.getProperty(key, defaultValue);
      props.remove(key);
      return value;
    }
  }

  /** Headers that are added to the Kafka records. */
  public enum Header {
    SPEC_VERSION("spec-version"),
    MAX_API_VERSION("max-api-version"),
    REPOSITORY_ID("repository-id"),
    INITIATOR("initiator"),
    EVENT_TYPE("event-type"),
    EVENT_CREATION_TIME("event-creation-time"),
    COMMIT_CREATION_TIME("commit-creation-time"),
    ;

    private final String key;

    Header(String key) {
      this.key = key;
    }

    public String getKey() {
      return key;
    }

    public void addValue(Headers headers, byte[] value) {
      headers.add(key, value);
    }

    public byte[] getValue(Headers headers) {
      org.apache.kafka.common.header.Header header = headers.lastHeader(key);
      if (header == null) {
        return null;
      }
      return header.value();
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaEventSubscriber.class);
  private static final Pattern COMMA = Pattern.compile(",");

  private final Properties props;
  private final Function<Properties, Producer<String, T>> producerFactory;

  private byte[] serverSpecVersion;
  private byte[] maxApiVersion;

  private EventFilter eventFilter;
  protected String topicName;
  protected Producer<String, T> producer;

  /**
   * Creates a new {@link KafkaAvroEventSubscriber} using the configuration file specified by the
   * system property {@value #NESSIE_KAFKA_PROPERTIES_FILE_SYS_PROP} or the environment variable
   * {@value #NESSIE_KAFKA_PROPERTIES_FILE_ENV_VAR}.
   *
   * @throws UncheckedIOException if the configuration file cannot be read.
   */
  public AbstractKafkaEventSubscriber() throws UncheckedIOException {
    this(
        System.getProperty(
            NESSIE_KAFKA_PROPERTIES_FILE_SYS_PROP,
            System.getenv(NESSIE_KAFKA_PROPERTIES_FILE_ENV_VAR) == null
                ? NESSIE_KAFKA_PROPERTIES_FILE_DEFAULT
                : System.getenv(NESSIE_KAFKA_PROPERTIES_FILE_ENV_VAR)));
  }

  public AbstractKafkaEventSubscriber(String location) throws UncheckedIOException {
    this(loadProperties(location));
  }

  public AbstractKafkaEventSubscriber(Properties props) {
    this(props, KafkaProducer::new);
  }

  /**
   * Creates a new {@link AbstractKafkaEventSubscriber} using the given {@link Properties} and
   * {@link Producer} factory.
   *
   * <p>Intended for testing mostly.
   */
  public AbstractKafkaEventSubscriber(
      Properties props, Function<Properties, Producer<String, T>> producerFactory) {
    this.props = props;
    this.producerFactory = producerFactory;
  }

  @Override
  public boolean isBlocking() {
    // We don't wait for the broker acknowledgement, so we can tell Nessie
    // that we don't do blocking I/O.
    return false;
  }

  @Override
  public synchronized void onSubscribe(EventSubscription subscription) {
    if (producer != null) {
      // This should never happen, but just in case...
      throw new IllegalStateException("Already subscribed");
    }
    // Use this method to initialize the subscriber and create the Kafka producer.
    // You can also use the passed-in EventSubscription to inspect the Nessie system configuration.
    serverSpecVersion = subscription.getSystemConfiguration().getSpecVersion().getBytes(UTF_8);
    maxApiVersion =
        new byte[] {(byte) subscription.getSystemConfiguration().getMaxSupportedApiVersion()};
    Set<String> repositoryIds =
        COMMA
            .splitAsStream(ConfigOption.REPOSITORY_IDS.getValue(props))
            .filter(s -> !s.isEmpty())
            .map(String::trim)
            .map(s -> s.replace("'", ""))
            .collect(Collectors.toSet());
    // If the repositoryIds set is empty, we want to watch all repositories.
    // Otherwise, we only want to watch to the repositories in the set, and discard all others
    // (Nessie won't even send events from such repositories to us).
    eventFilter =
        repositoryIds.isEmpty()
            ? EventFilter.all()
            : e -> repositoryIds.contains(e.getRepositoryId());
    topicName = ConfigOption.TOPIC_NAME.getValue(props);
    producer = producerFactory.apply(props);
    LOGGER.info(
        "Nessie-Kafka events subscriber ready, publishing to topic '{}', watching repositories: {}",
        topicName,
        repositoryIds.isEmpty()
            ? "all"
            : repositoryIds.stream().map(s -> "'" + s + "'").collect(Collectors.joining(",")));
    LOGGER.info("Nessie subscription id: {}", subscription.getId());
  }

  @Override
  public synchronized void close() {
    // Called when the Nessie server is stopped. Use it to close any resources, such as the Kafka
    // producer.
    if (producer == null) {
      return;
    }
    producer.flush();
    producer.close();
    producer = null;
    LOGGER.info("Nessie-Kafka events subscriber closed");
  }

  @Override
  public final EventFilter getEventFilter() {
    return eventFilter;
  }

  /**
   * Sends the event to Kafka.
   *
   * <p>This method is invoked on a Nessie thread, so it should not block for too long. It is
   * therefore advisable to use either the fire-and-forget method, or the so-called asynchronous
   * send method with callback, so that the Nessie server is not blocked by a slow Kafka broker.
   *
   * <p>If you really need to wait for the result, you can use {@link Future#get()} here. However,
   * you should report this to Nessie by returning true from the {@link #isBlocking()} method. Also:
   * note that the wait time will include retries, if any.
   *
   * <p>In case the {@link Producer#send(ProducerRecord)} method throws immediately (for example,
   * because the record could not be serialized), it is advisable to re-throw the exception, so that
   * the Nessie server can react to it. For example, the server could record the exception and
   * cancel the subscription after a configurable number of consecutive failures.
   */
  protected void fireEvent(Event upstreamEvent, T downstreamEvent) {
    ProducerRecord<String, T> record =
        new ProducerRecord<>(topicName, eventKey(upstreamEvent), downstreamEvent);
    addCommonHeaders(upstreamEvent, record);
    try {
      Future<RecordMetadata> ignored = producer.send(record, this::onResult);
    } catch (RuntimeException e) {
      LOGGER.error("Failed to send event to Kafka", e);
      throw e;
    }
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
  protected String eventKey(Event event) {
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

  protected void addCommonHeaders(Event event, ProducerRecord<?, ?> record) {
    Headers headers = record.headers();
    Header.SPEC_VERSION.addValue(headers, serverSpecVersion);
    Header.MAX_API_VERSION.addValue(headers, maxApiVersion);
    Header.EVENT_TYPE.addValue(headers, event.getType().name().getBytes(UTF_8));
    Header.REPOSITORY_ID.addValue(headers, event.getRepositoryId().getBytes(UTF_8));
    event
        .getEventInitiator()
        .ifPresent(user -> Header.INITIATOR.addValue(headers, user.getBytes(UTF_8)));
    Header.EVENT_CREATION_TIME.addValue(
        headers, event.getEventCreationTimestamp().toString().getBytes(UTF_8));
    // For commits and operations, add the commit creation time as well
    if (event instanceof CommitEvent) {
      org.projectnessie.events.api.CommitEvent commitEvent =
          (org.projectnessie.events.api.CommitEvent) event;
      Instant commitTime = commitEvent.getCommitMeta().getCommitTime();
      if (commitTime != null) {
        Header.COMMIT_CREATION_TIME.addValue(headers, commitTime.toString().getBytes(UTF_8));
      }
    } else if (event instanceof ContentEvent) {
      ContentEvent contentEvent = (ContentEvent) event;
      Header.COMMIT_CREATION_TIME.addValue(
          headers, contentEvent.getCommitCreationTimestamp().toString().getBytes(UTF_8));
    }
  }

  /**
   * Callback for the asynchronous send method. This method is called on a Kafka producer thread.
   * Use this to increment metrics, or trigger a circuit breaker if too many events fail to be
   * written to Kafka. It is not recommended to throw an exception here.
   *
   * <p>The {@value org.apache.kafka.clients.producer.ProducerConfig#ACKS_CONFIG} setting of the
   * Kafka producer determines when this method is called. If it is set to {@code 0}, this method is
   * called immediately after send. If it is set to {@code 1}, this method is called after the event
   * has been written to the Kafka broker. If it is set to {@code all}, this method is called after
   * the event has been written to the Kafka broker and replicated to all in-sync replicas.
   *
   * @param metadata The metadata of the record that was written to Kafka.
   * @param error The exception that occurred, or {@code null} if the event was written
   *     successfully.
   */
  protected void onResult(RecordMetadata metadata, Exception error) {
    if (error == null) {
      // Do NOT enable this log statement in production!
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Event written to Kafka topic '{}' partition {} offset {}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset());
      }
    } else {
      LOGGER.error("Failed to write event to Kafka topic", error);
    }
  }

  private static Properties loadProperties(String location) throws UncheckedIOException {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(location), UTF_8)) {
      Properties props = new Properties();
      props.load(reader);
      return props;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read Kafka properties file: " + location, e);
    }
  }
}

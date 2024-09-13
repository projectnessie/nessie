# Nessie Events SPI - Reference Implementation

This module contains reference implementations of Nessie's events notification system SPI module.

## Overview

Two implementations of the SPI module can be found in this module:

1. `org.projectnessie.events.ri.console.PrintingEventSubscriber` - a (very) simple implementation 
   that prints events to the console.
2. `org.projectnessie.events.ri.kafka.KafkaEventSubscriber` - an implementation that publishes 
   events to a Kafka topic.

## PrintingEventSubscriber

The `PrintingEventSubscriber` implementation is a very simple implementation that prints events to the console. It is
intended to illustrate the basic concepts of an SPI implementation. Do NOT use in production.

## KafkaEventSubscriber

`KafkaEventSubscriber` illustrates how to handle events in a more realistic scenario. It is intended to be used as a
starting point for implementing a real subscriber based on [Apache Kafka].

[Apache Kafka]:https://kafka.apache.org/

### Configuration

The `KafkaEventSubscriber` implementation reads its configuration from a properties file. The default location of this
file is `./nessie-kafka.properties`, but this can be changed by setting the `nessie.events.config.file` system property
or the `NESSIE_EVENTS_CONFIG_FILE` environment variable to a different location.

The properties file should contain all the desired Kafka producer configuration options, as well as the following
subscriber-specific options:

- `nessie.events.topic` - the name of the Kafka topic to publish events to. Defaults to `nessie-events`.
- `nessie.events.repository-ids` - a comma-separated list of repository ids to watch. If not set, all repositories will
  be watched. See below for more details.

A template configuration file can be found in `src/main/resources/nessie-kafka.template.properties`.

### Serialization and deserialization

`KafkaEventSubscriber` uses the [Avro] library to define a domain model that is specific to the subscriber, and is not
tied in any way to Nessie's. This is indeed the recommended approach, as it allows the subscriber to define its own
domain model and schema, and to evolve them independently of Nessie.

[Avro]:https://avro.apache.org/

The domain model is defined in the `src/main/avro/NessieEvents.avdl` file. The Avro compiler and its Gradle plugin are
used during build to generate Java classes from the Avro schema files. The generated classes are located in the
`build/generated-main-avro-java` directory.

Note: for the consumer to be able to deserialize the messages into the same generated classes, it must be configured
with `specific.avro.reader=true`.

### Schema and polymorphism

Nessie's Events API is polymorphic, in the sense that it supports different types of events. The `KafkaEventSubscriber`
implementation illustrates how to handle different types of events in the same subscriber, pushing to a single Kafka
topic. 

In this case, the subscriber aggregates Nessie's event types into three kinds of Avro messages: `CommitEvent`,
`ReferenceEvent`, and `OperationEvent`.

- `CommitEvent` is generated when a commit is made to a branch and corresponds to Nessie's `COMMIT` event type;
- `ReferenceEvent` is generated when a branch or tag is created, updated, or deleted, and corresponds to Nessie's
  `REFERENCE_CREATED`, `REFERENCE_UPDATED`, and `REFERENCE_DELETED` event types;
- `OperationEvent` is generated when a PUT or a DELETE operation occurs, and corresponds to Nessie's
  `CONTENT_STORED` and `CONTENT_REMOVED` event types.

In this example, the destination Kafka topic is going to receive messages of three different types. This is a common
scenario in Kafka, but requires some extra work to be done on both the producer and consumer sides, especially with
respect to schema handling. The strategy we chose to use here is outlined in [Martin Kleppmann's excellent article] and
relies on the subject-name strategy called `TopicRecordNameStrategy`. Other strategies are available, see [Robert
Yokota's follow-up article] for a good overview.

[Martin Kleppmann's excellent article]:https://www.confluent.io/blog/put-several-event-types-kafka-topic/
[Robert Yokota's follow-up article]:https://www.confluent.io/blog/multiple-event-types-in-the-same-kafka-topic/

### Topic partitioning and message ordering

The `KafkaEventSubscriber` implementation illustrates how to partition the destination Kafka topic _by repository id and
reference name_: this should allow downstream consumers to process events for a given reference in the order they were
produced.

Note that the order in which events are produced to a topic does not necessarily reflect the order in which Nessie's
version store recorded them. As stated in Nessie's [Events API design document], strict ordering of events is not
guaranteed. More specifically, for two events A and B that happened sequentially in a very short timeframe on the same
reference, Nessie may notify the subscriber of B first, than A. In this case, the subscriber would put B in the topic
before A, resulting in out-of-order sequencing.

This only happens for very close events, and for most use cases this shouldn't be a problem; but if strict ordering is
required, consider using more advanced techniques, such as [Kafka Streams] to re-order the messages, based e.g. on the
commit timestamp.

[Events API design document]:https://github.com/projectnessie/nessie/blob/nessie-0.60.1/design/events.md?plain=1#L364
[Kafka Streams]:https://kafka.apache.org/documentation/streams/

### Producer-side filtering

Producer-side filtering is possible by changing the implementation of either the `getEventFilter()` or
`getEventTypeFilter()` methods.

To illustrate this, `KafkaEventSubscriber` uses its `EventTypeFilter` to completely ignore merge and transplant events.
(Also note: merge and transplant events also trigger commit events, so it's still possible to capture what happened in a
branch by listening to commit events only.)

The subscriber is further configured to optionally watch just a few repository ids, or all of them. This is done by
setting the `nessie.events.repository-ids` configuration option, and is implemented by modifying the subscriber's
`EventFilter`. Again, this is purely illustrative. Other ways of filtering events on the subscriber side are possible,
for example one could configure the subscribe to watch only certain branches or tags.

### Using headers efficiently

The `KafkaEventSubscriber` implementation also illustrates how to use Kafka headers to pass additional information along
with the message. In this case, the subscriber adds the repository id, the user and the event creation timestamp to the
message headers, allowing consumers to filter or sort messages based on these values.

### Other considerations

The `KafkaEventSubscriber` implementation also illustrates how to handle events in a non-blocking way, by publishing
messages without waiting for the broker's acknowledgement. This is achieved by calling the `KafkaProducer#send` method
in asynchronous mode, with a completion callback. If your implementation needs to wait for the broker's acknowledgement
synchronously, then you should report to Nessie that this subscriber is going to be blocking by returning `true` from
the `isBlocking()` method.

Finally, the `KafkaEventSubscriber` implementation does not do any initialization in its constructor. This is because
the constructor is called during the SPI module's initialization, which happens before Nessie is fully started. Instead,
the subscriber's initialization should be done in the `onSubscribe` method, which is called after Nessie has started
(but before receiving events).

### Testing

The `org.projectnessie.events.ri.kafka.ITKafkaEventSubscriber` integration test provides a real-world example of how to
test the subscriber. It uses a real Kafka broker and schema registry. It then plays a sequence of events that is
representative of what Nessie would send to the subscriber when a user creates a branch, commits to it, and then deletes
it. Finally, it checks that the subscriber has received the expected messages.

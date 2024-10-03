# Nessie Events SPI - Reference Implementation

This module contains reference implementations of Nessie's events notification system SPI module.

## Overview

The following implementations of the SPI module can be found in this module:

1. `PrintingEventSubscriber` - a (very) simple implementation that prints events to the console.
2. Messaging subscribers:
    1. `KafkaAvroEventSubscriber` - publishes events to a Kafka topic using Avro.
    2. `KafkaJsonEventSubscriber` - publishes events to a Kafka topic using JSON.
    3. `NatsJsonEventSubscriber` - publishes events to a NATS stream using JSON.

This module uses Quarkus, since Nessie servers are based on Quarkus. The [Quarkus Messaging
extension] is particularly useful in this context, as it unifies the way to handle messages in
Quarkus, regardless of the messaging system used. Two actual messaging extensions are used in this
module: the [Apache Kafka extension] and the [Quarkus NATS extension]. Please make sure to read the
documentation of these extensions to understand how they work.

[Quarkus Messaging extension]:https://quarkus.io/guides/messaging
[Apache Kafka extension]:https://quarkus.io/guides/kafka
[Quarkus NATS extension]:https://docs.quarkiverse.io/quarkus-reactive-messaging-nats-jetstream/dev

### Printing Subscriber

The `PrintingEventSubscriber` implementation is a very simple implementation that prints events to
the console. It is intended to illustrate the basic concepts of an SPI implementation. Do NOT use in
production.

### Messaging Subscribers

`KafkaAvroEventSubscriber` illustrates how to handle events using Avro to serialize and deserialize
messages, and using a Schema Registry.

`KafkaJsonEventSubscriber` illustrates how to handle events using JSON to serialize and deserialize
events.

`NatsJsonEventSubscriber` illustrates how to handle events using JSON to serialize and deserialize
events, and using NATS as the messaging system.

These subscribers are intended to be used as a starting point for implementing a real-world
subscriber based on [Apache Kafka](https://kafka.apache.org/) and [NATS](https://nats.io/).

## Configuration

All subscribers, producers, as well as the middleware (brokers), can be configured through the
`application.properties` file. Read the comments in the file for more information.

## Serialization and deserialization

### Avro

`KafkaAvroEventSubscriber` uses the [Avro] library to define a domain model that is specific to the
subscriber, and is not tied in any way to Nessie's. This approach allows to decouple Nessie's domain
model from the subscriber's domain model, at the cost of having to maintain the Avro schema files.

[Avro]:https://avro.apache.org/

The domain model is defined in the `src/main/avro/NessieEvents.avdl` file. There are several Avro
messages declared in this file, and the `KafkaAvroEventSubscriber` implementation illustrates how to
handle different types of events being pushed to a single Kafka topic.

Producing records of different types to the same topic is a common pattern in Kafka, but requires
some extra work on both the producer and consumer sides, especially with respect to schema handling.
Depending on the schema registry used, various strategies for retrieving the right schema for a
message can be used. Have a look at the [Using Apache Kafka with Schema Registry and Avro] guide for
general guidelines.

Note: for the consumer to be able to deserialize the messages into the same generated classes, it
must be configured to use Avro's `SpecificRecord` serialization. This is done e.g. with the
`specific.avro.reader` property (if using Confluent) or the
`apicurio.registry.use-specific-avro-reader` property (if using Apicurio). See the
`application.properties` file for more information.

[Using Apache Kafka with Schema Registry and Avro]:https://quarkus.io/guides/kafka-schema-registry-avro

### JSON

Both `KafkaJsonEventSubscriber` and `NatsJsonEventSubscriber` use Jackson to serialize and
deserialize events to and from JSON. This is a simpler approach than using Avro, but it requires the
subscriber to have a good understanding of Nessie's domain model, as the JSON representation of the
events is directly tied to it.

Also, Nessie's model API makes usage of Jackson views; depending on the active view, the serialized
payload may change. The serialization and deserialization of the events must take this into account.
See `KafkaJsonEventSerialization` and `ViewAwareJsonMessageFactory` for more details.

## Kafka topic partitioning and message ordering

`KafkaAvroEventSubscriber` and `KafkaJsonEventSubscriber` both illustrate how to partition the
destination Kafka topic _by repository id and reference name_: this should allow downstream
consumers to process events for a given reference in the order they were produced.

Note that the order in which events are produced to a topic does not necessarily reflect the order
in which Nessie's version store recorded them. As stated in Nessie's [Events API design document],
strict ordering of events is not guaranteed. More specifically, for two events A and B that happened
sequentially in a very short timeframe on the same reference, Nessie may notify the subscriber of B
first, than A. In this case, the subscriber would put B in the topic before A, resulting in
out-of-order sequencing.

This only happens for very close events, and for most use cases this shouldn't be a problem; but if
strict ordering is required, consider using more advanced techniques, such as [Kafka Streams] to
re-order the messages, based e.g. on the commit timestamp.

[Events API design document]:https://github.com/projectnessie/nessie/blob/nessie-0.60.1/design/events.md?plain=1#L364
[Kafka Streams]:https://kafka.apache.org/documentation/streams/

## NATS subject configuration

`NatsJsonEventSubscriber` illustrates how to configure the NATS subject with hierarchical subjects.
It publishes to the following subjects:

* `nessie.events.commit`
* `nessie.events.commit.content.stored`
* `nessie.events.commit.content.removed`
* `nessie.events.reference.created`
* `nessie.events.reference.updated`
* `nessie.events.reference.deleted`
* `nessie.events.merge`
* `nessie.events.transplant`

If a client want to subscribe to all events, it can subscribe to `nessie.events.>`. This is a
convenient way to subscribe to all events, without having to subscribe to each individual subject.
Conversely, if a client is only interested in a specific type of event, it can subscribe to the
specific subject, e.g. `nessie.events.commit` would only receive commit events, as well as
content-related events.

This is of course just an example, and the subject hierarchy can be changed to suit the client's
needs.

## Producer-side filtering

Producer-side filtering is possible by changing the implementation of either the `getEventFilter()`
or `getEventTypeFilter()` methods.

To illustrate this, all messaging subscribers can be configured to only watch certain event types;
see the `application.properties` file for more information, and also the
`MessagingEventSubscribersConfig` configuration class. Filtering out event types is done by
modifying the subscriber's `EventTypeFilter`.

The subscribers can also be configured to optionally watch just a few repository ids, or all of
them. This is again done by configuration, and is implemented by modifying the subscriber's
`EventFilter`.

These filtering techniques are purely illustrative. Other ways of filtering events on the subscriber
side are possible, for example one could configure the subscribe to watch only certain branches or
tags.

## Using headers efficiently

ALl messaging subscribers show how to use Kafka headers to pass additional information along with
the message. In this case, the subscriber adds the repository id, the user and the event creation
timestamp to the message headers, allowing consumers to filter or sort messages based on these
values.

## Other considerations

The example subscribers also illustrate how to handle events in a non-blocking way, by publishing
messages without waiting for the broker's acknowledgement. If your implementation needs to wait for
the broker's acknowledgement synchronously, then you should report to Nessie that this subscriber is
going to be blocking by returning `true` from the `isBlocking()` method.

Finally, `EventSubscriber` implementations are discovered automatically by Quarkus, and therefore
must be declared as application-scoped  CDI beans. This is done by annotating the implementation
class with `@ApplicationScoped` (or `@Singleton`).

## Testing

The tests provide a real-world example of how to test the subscriber. They rely on Quarkus
DevServices to start a Kafka and a NATS broker, and also an Apicurio schema registry.

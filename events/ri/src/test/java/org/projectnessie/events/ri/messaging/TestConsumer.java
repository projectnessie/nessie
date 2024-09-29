/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.events.ri.messaging;

import static org.awaitility.Awaitility.await;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.avro.specific.SpecificRecord;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.projectnessie.events.api.Event;

@ApplicationScoped
public class TestConsumer {

  private final List<Message<Event>> natsMessages = new CopyOnWriteArrayList<>();
  private final List<Message<Event>> jsonMessages = new CopyOnWriteArrayList<>();
  private final List<Message<SpecificRecord>> avroMessages = new CopyOnWriteArrayList<>();

  @Incoming("nessie-nats-json-consumer")
  public CompletionStage<Void> onNatsJsonMessage(Message<Event> message) {
    natsMessages.add(message);
    return message.ack();
  }

  @Incoming("nessie-kafka-json-consumer")
  public CompletionStage<Void> onKafkaJsonMessage(Message<Event> message) {
    jsonMessages.add(message);
    return message.ack();
  }

  @Incoming("nessie-kafka-avro-consumer")
  public CompletionStage<Void> onKafkaAvroMessage(Message<SpecificRecord> message) {
    avroMessages.add(message);
    return message.ack();
  }

  public void reset() {
    natsMessages.clear();
    jsonMessages.clear();
    avroMessages.clear();
  }

  public List<Message<Event>> awaitNatsMessages(int numMessages) {
    await().until(() -> natsMessages.size() == numMessages);
    return List.copyOf(natsMessages);
  }

  public List<Message<Event>> awaitKafkaJsonMessages(int numMessages) {
    await().until(() -> jsonMessages.size() == numMessages);
    return List.copyOf(jsonMessages);
  }

  public List<Message<SpecificRecord>> awaitKafkaAvroMessages(int numMessages) {
    await().until(() -> avroMessages.size() == numMessages);
    return List.copyOf(avroMessages);
  }
}

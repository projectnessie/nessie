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
package org.projectnessie.events.ri.messaging.kafka.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.example.nessie.events.generated.OperationEvent;
import com.example.nessie.events.generated.OperationEventType;
import com.example.nessie.events.generated.ReferenceEvent;
import com.example.nessie.events.generated.ReferenceEventType;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.ri.messaging.kafka.AbstractKafkaEventSubscriberTests;
import org.projectnessie.events.spi.EventSubscriber;

@QuarkusTest
public class TestKafkaAvroEventSubscriber
    extends AbstractKafkaEventSubscriberTests<SpecificRecord> {

  @Inject KafkaAvroEventSubscriber subscriber;

  @Override
  protected EventSubscriber subscriber() {
    return subscriber;
  }

  @Override
  protected Message<SpecificRecord> receive() {
    return consumer.awaitKafkaAvroMessages(1).getFirst();
  }

  @Override
  protected void checkMessage(Message<SpecificRecord> actual, Event upstreamEvent) {
    super.checkMessage(actual, upstreamEvent);
    checkPayload(actual, upstreamEvent);
  }

  private void checkPayload(Message<SpecificRecord> actual, Event upstreamEvent) {
    switch (upstreamEvent.getType()) {
      case COMMIT -> checkCommit(actual, upstreamEvent);
      case CONTENT_STORED -> checkContentStored(actual, upstreamEvent);
      case CONTENT_REMOVED -> checkContentRemoved(actual, upstreamEvent);
      case REFERENCE_CREATED -> checkReferenceCreated(actual, upstreamEvent);
      case REFERENCE_DELETED -> checkReferenceDeleted(actual, upstreamEvent);
      case REFERENCE_UPDATED -> checkReferenceUpdated(actual, upstreamEvent);
      case MERGE -> checkMerge(actual, upstreamEvent);
      case TRANSPLANT -> checkTransplant(actual, upstreamEvent);
      default ->
          throw new IllegalArgumentException("Unexpected event type: " + upstreamEvent.getType());
    }
  }

  private void checkCommit(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
        .asInstanceOf(type(com.example.nessie.events.generated.CommitEvent.class))
        .extracting(
            com.example.nessie.events.generated.CommitEvent::getId,
            com.example.nessie.events.generated.CommitEvent::getHashBefore,
            com.example.nessie.events.generated.CommitEvent::getHashAfter,
            com.example.nessie.events.generated.CommitEvent::getReference)
        .containsExactly(upstreamEvent.getId(), "hashBefore", "hashAfter", "branch1");
  }

  private void checkContentStored(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
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
            upstreamEvent.getId(),
            "hash",
            "branch1",
            "folder1.folder2.table1",
            "ICEBERG_TABLE",
            "id",
            Map.of(
                "metadataLocation", "metadataLocation",
                "snapshotId", "1",
                "schemaId", "2",
                "specId", "3",
                "sortOrderId", "4"));
  }

  private void checkContentRemoved(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
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
            upstreamEvent.getId(),
            "hash",
            "branch1",
            "folder1.folder2.table1",
            null,
            null,
            Map.of());
  }

  private void checkReferenceCreated(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
        .asInstanceOf(type(ReferenceEvent.class))
        .extracting(
            ReferenceEvent::getId,
            ReferenceEvent::getType,
            ReferenceEvent::getHashBefore,
            ReferenceEvent::getHashAfter,
            ReferenceEvent::getReference)
        .containsExactly(
            upstreamEvent.getId(), ReferenceEventType.CREATED, null, "hashAfter", "branch1");
  }

  private void checkReferenceDeleted(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
        .asInstanceOf(type(ReferenceEvent.class))
        .extracting(
            ReferenceEvent::getId,
            ReferenceEvent::getType,
            ReferenceEvent::getHashBefore,
            ReferenceEvent::getHashAfter,
            ReferenceEvent::getReference)
        .containsExactly(
            upstreamEvent.getId(), ReferenceEventType.DELETED, "hashBefore", null, "branch1");
  }

  private void checkReferenceUpdated(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
        .asInstanceOf(type(ReferenceEvent.class))
        .extracting(
            ReferenceEvent::getId,
            ReferenceEvent::getType,
            ReferenceEvent::getHashBefore,
            ReferenceEvent::getHashAfter,
            ReferenceEvent::getReference)
        .containsExactly(
            upstreamEvent.getId(),
            ReferenceEventType.REASSIGNED,
            "hashBefore",
            "hashAfter",
            "branch1");
  }

  private void checkMerge(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
        .asInstanceOf(type(com.example.nessie.events.generated.MergeEvent.class))
        .extracting(
            com.example.nessie.events.generated.MergeEvent::getId,
            com.example.nessie.events.generated.MergeEvent::getHashBefore,
            com.example.nessie.events.generated.MergeEvent::getHashAfter,
            com.example.nessie.events.generated.MergeEvent::getCommonAncestorHash,
            com.example.nessie.events.generated.MergeEvent::getSourceHash,
            com.example.nessie.events.generated.MergeEvent::getSourceReference,
            com.example.nessie.events.generated.MergeEvent::getTargetReference)
        .containsExactly(
            upstreamEvent.getId(),
            "hashBefore",
            "hashAfter",
            "commonAncestorHash",
            "sourceHash",
            "branch1",
            "branch2");
  }

  private void checkTransplant(Message<SpecificRecord> actual, Event upstreamEvent) {
    assertThat(actual.getPayload())
        .asInstanceOf(type(com.example.nessie.events.generated.TransplantEvent.class))
        .extracting(
            com.example.nessie.events.generated.TransplantEvent::getId,
            com.example.nessie.events.generated.TransplantEvent::getHashBefore,
            com.example.nessie.events.generated.TransplantEvent::getHashAfter,
            com.example.nessie.events.generated.TransplantEvent::getCommitCount,
            com.example.nessie.events.generated.TransplantEvent::getTargetReference)
        .containsExactly(upstreamEvent.getId(), "hashBefore", "hashAfter", 3, "branch2");
  }
}

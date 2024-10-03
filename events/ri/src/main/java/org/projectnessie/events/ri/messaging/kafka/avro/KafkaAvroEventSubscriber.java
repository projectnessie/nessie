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

import com.example.nessie.events.generated.CommitEvent;
import com.example.nessie.events.generated.MergeEvent;
import com.example.nessie.events.generated.OperationEvent;
import com.example.nessie.events.generated.OperationEventType;
import com.example.nessie.events.generated.ReferenceEvent;
import com.example.nessie.events.generated.ReferenceEventType;
import com.example.nessie.events.generated.TransplantEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig;
import org.projectnessie.events.ri.messaging.config.MessagingEventSubscribersConfig.EventSubscriberConfig;
import org.projectnessie.events.ri.messaging.kafka.AbstractKafkaEventSubscriber;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;

/** An {@link EventSubscriber} that publishes events to a Kafka topic using Avro. */
@ApplicationScoped
public class KafkaAvroEventSubscriber extends AbstractKafkaEventSubscriber<SpecificRecord> {

  public static final String CHANNEL = "nessie-kafka-avro";

  /** Constructor required by CDI. */
  @SuppressWarnings("unused")
  public KafkaAvroEventSubscriber() {
    super(null, null);
  }

  @Inject
  public KafkaAvroEventSubscriber(
      @Channel(CHANNEL) Emitter<SpecificRecord> emitter, MessagingEventSubscribersConfig config) {
    super(emitter, config.subscribers().getOrDefault(CHANNEL, EventSubscriberConfig.EMPTY));
  }

  @Override
  public void onCommit(org.projectnessie.events.api.CommitEvent upstreamEvent) {
    CommitEvent downstreamEvent =
        new CommitEvent(
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHashBefore(),
            upstreamEvent.getHashAfter());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onMerge(org.projectnessie.events.api.MergeEvent event) {
    MergeEvent downstreamEvent =
        new MergeEvent(
            event.getId(),
            event.getSourceReference().getName(),
            event.getTargetReference().getName(),
            event.getSourceHash(),
            event.getHashBefore(),
            event.getHashAfter(),
            event.getCommonAncestorHash());
    fireEvent(event, downstreamEvent);
  }

  @Override
  public void onTransplant(org.projectnessie.events.api.TransplantEvent event) {
    TransplantEvent downstreamEvent =
        new TransplantEvent(
            event.getId(),
            event.getTargetReference().getName(),
            event.getHashBefore(),
            event.getHashAfter(),
            event.getCommitCount());
    fireEvent(event, downstreamEvent);
  }

  @Override
  public void onContentStored(org.projectnessie.events.api.ContentStoredEvent upstreamEvent) {
    OperationEvent downstreamEvent =
        new OperationEvent(
            OperationEventType.PUT,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHash(),
            upstreamEvent.getContentKey().toCanonicalString(),
            upstreamEvent.getContent().getId(),
            upstreamEvent.getContent().getType().toString(),
            contentProperties(upstreamEvent.getContent()));
    fireEvent(upstreamEvent, downstreamEvent);
  }

  private Map<String, String> contentProperties(Content content) {
    if (content instanceof IcebergTable icebergTable) {
      return Map.of(
          "metadataLocation", icebergTable.getMetadataLocation(),
          "snapshotId", String.valueOf(icebergTable.getSnapshotId()),
          "specId", String.valueOf(icebergTable.getSpecId()),
          "sortOrderId", String.valueOf(icebergTable.getSortOrderId()),
          "schemaId", String.valueOf(icebergTable.getSchemaId()));
    } else if (content instanceof IcebergView icebergView) {
      return Map.of(
          "metadataLocation", icebergView.getMetadataLocation(),
          "schemaId", String.valueOf(icebergView.getSchemaId()));
    }
    return Map.of();
  }

  @Override
  public void onContentRemoved(org.projectnessie.events.api.ContentRemovedEvent upstreamEvent) {
    OperationEvent downstreamEvent =
        new OperationEvent(
            OperationEventType.DELETE,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHash(),
            upstreamEvent.getContentKey().toCanonicalString(),
            null,
            null,
            Map.of());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onReferenceCreated(org.projectnessie.events.api.ReferenceCreatedEvent upstreamEvent) {
    ReferenceEvent downstreamEvent =
        new ReferenceEvent(
            ReferenceEventType.CREATED,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            null,
            upstreamEvent.getHashAfter());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onReferenceUpdated(org.projectnessie.events.api.ReferenceUpdatedEvent upstreamEvent) {
    ReferenceEvent downstreamEvent =
        new ReferenceEvent(
            ReferenceEventType.REASSIGNED,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHashBefore(),
            upstreamEvent.getHashAfter());
    fireEvent(upstreamEvent, downstreamEvent);
  }

  @Override
  public void onReferenceDeleted(org.projectnessie.events.api.ReferenceDeletedEvent upstreamEvent) {
    ReferenceEvent downstreamEvent =
        new ReferenceEvent(
            ReferenceEventType.DELETED,
            upstreamEvent.getId(),
            upstreamEvent.getReference().getName(),
            upstreamEvent.getHashBefore(),
            null);
    fireEvent(upstreamEvent, downstreamEvent);
  }
}

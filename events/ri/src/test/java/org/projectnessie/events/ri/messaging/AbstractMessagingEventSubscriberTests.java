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

import jakarta.inject.Inject;
import java.time.Instant;
import java.util.UUID;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableMergeEvent;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.ImmutableReferenceUpdatedEvent;
import org.projectnessie.events.api.ImmutableTransplantEvent;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.ImmutableEventSubscription;
import org.projectnessie.events.spi.ImmutableEventSystemConfiguration;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractMessagingEventSubscriberTests<T> {

  protected final Instant now = Instant.parse("2024-01-01T12:34:56Z");
  protected final Reference branch1 = Branch.of("branch1", "cafebabe");
  protected final Reference branch2 = Branch.of("branch2", "deadbeef");

  @Inject protected TestConsumer consumer;

  private EventSubscriber eventSubscriber;

  @BeforeAll
  public void setUpSubscriber() {
    eventSubscriber = subscriber();
    eventSubscriber.onSubscribe(createSubscription());
  }

  @AfterEach
  public void resetConsumer() {
    consumer.reset();
  }

  @AfterAll
  public void closeSubscriber() throws Exception {
    eventSubscriber.close();
  }

  @Test
  public void testCommit() {

    CommitEvent upstreamEvent =
        ImmutableCommitEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .reference(branch1)
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .commitTime(now)
                    .authorTime(now)
                    .addAllAuthors("author")
                    .message("message")
                    .build())
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testMerge() {

    MergeEvent upstreamEvent =
        ImmutableMergeEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .sourceHash("sourceHash")
            .commonAncestorHash("commonAncestorHash")
            .sourceReference(branch1)
            .targetReference(branch2)
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testTransplant() {

    TransplantEvent upstreamEvent =
        ImmutableTransplantEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .targetReference(branch2)
            .commitCount(3)
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testContentStored() {

    ContentStoredEvent upstreamEvent =
        ImmutableContentStoredEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .commitCreationTimestamp(now)
            .hash("hash")
            .reference(branch1)
            .contentKey(ContentKey.of("folder1", "folder2", "table1"))
            .content(IcebergTable.of("metadataLocation", 1L, 2, 3, 4, "id"))
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testContentRemoved() {

    ContentRemovedEvent upstreamEvent =
        ImmutableContentRemovedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .commitCreationTimestamp(now)
            .hash("hash")
            .reference(branch1)
            .contentKey(ContentKey.of("folder1", "folder2", "table1"))
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testReferenceCreated() {

    ReferenceCreatedEvent upstreamEvent =
        ImmutableReferenceCreatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch1)
            .hashAfter("hashAfter")
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testReferenceDeleted() {

    ReferenceDeletedEvent upstreamEvent =
        ImmutableReferenceDeletedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch1)
            .hashBefore("hashBefore")
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  @Test
  public void testReferenceUpdated() {

    ReferenceUpdatedEvent upstreamEvent =
        ImmutableReferenceUpdatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .eventInitiator("alice")
            .eventCreationTimestamp(now)
            .reference(branch1)
            .hashBefore("hashBefore")
            .hashAfter("hashAfter")
            .build();

    send(upstreamEvent);
    Message<T> actual = receive();
    checkMessage(actual, upstreamEvent);
  }

  protected abstract EventSubscriber subscriber();

  protected void send(Event event) {
    eventSubscriber.onEvent(event);
  }

  protected abstract Message<T> receive();

  protected abstract void checkMessage(Message<T> actual, Event upstreamEvent);

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
}

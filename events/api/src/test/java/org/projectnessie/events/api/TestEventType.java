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
package org.projectnessie.events.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TestEventType {

  @Test
  void commit() {
    CommitEvent event =
        committingAttributes(CommitEvent.builder())
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .commitTime(Instant.now())
                    .committer("committer")
                    .message("message")
                    .authorTime(Instant.now())
                    .build())
            .build();
    assertThat(event.getType()).isEqualTo(EventType.COMMIT);
  }

  @Test
  void merge() {
    MergeEvent event =
        committingAttributes(MergeEvent.builder()).commonAncestorHash("hash0").build();
    assertThat(event.getType()).isEqualTo(EventType.MERGE);
  }

  @Test
  void transplant() {
    TransplantEvent event = committingAttributes(TransplantEvent.builder()).build();
    assertThat(event.getType()).isEqualTo(EventType.TRANSPLANT);
  }

  @Test
  void referenceCreated() {
    ReferenceCreatedEvent event =
        refAttributes(ReferenceCreatedEvent.builder()).hashAfter("hash2").build();
    assertThat(event.getType()).isEqualTo(EventType.REFERENCE_CREATED);
  }

  @Test
  void referenceUpdated() {
    ReferenceUpdatedEvent event =
        refAttributes(ReferenceUpdatedEvent.builder())
            .hashBefore("hash1")
            .hashAfter("hash2")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.REFERENCE_UPDATED);
  }

  @Test
  void referenceDeleted() {
    ReferenceDeletedEvent event =
        refAttributes(ReferenceDeletedEvent.builder()).hashBefore("hash1").build();
    assertThat(event.getType()).isEqualTo(EventType.REFERENCE_DELETED);
  }

  @Test
  void contentStored() {
    ContentStoredEvent event =
        contentAttributes(ContentStoredEvent.builder()).content(mock(Content.class)).build();
    assertThat(event.getType()).isEqualTo(EventType.CONTENT_STORED);
  }

  @Test
  void contentRemoved() {
    ContentRemovedEvent event = contentAttributes(ContentRemovedEvent.builder()).build();
    assertThat(event.getType()).isEqualTo(EventType.CONTENT_REMOVED);
  }

  static <B extends CommittingEvent.Builder<B, E>, E extends CommittingEvent>
      B committingAttributes(B builder) {
    return commonAttributes(
        builder
            .sourceReference("branch1")
            .targetBranch("branch2")
            .hashBefore("hash1")
            .hashAfter("hash2"));
  }

  static <B extends ReferenceEvent.Builder<B, E>, E extends ReferenceEvent> B refAttributes(
      B builder) {
    return commonAttributes(builder.referenceName("ref1").referenceType(ReferenceType.BRANCH));
  }

  static <B extends ContentEvent.Builder<B, E>, E extends ContentEvent> B contentAttributes(
      B builder) {
    return commonAttributes(
        builder.branch("branch1").hash("hash1").contentKey(ContentKey.of("ns", "table1")));
  }

  static <B extends Event.Builder<B, E>, E extends Event> B commonAttributes(B builder) {
    return builder
        .id(UUID.randomUUID())
        .repositoryId("repo1")
        .createdAt(Instant.now())
        .createdBy("Alice")
        .sentBy("Nessie");
  }
}

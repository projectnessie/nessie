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
package org.projectnessie.events.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.Reference;

class TestJsonSerde {

  private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new Jdk8Module());
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private static final UUID ID = UUID.fromString("58bdb0a2-20c0-4a6f-b6fe-abd4ebce2e96");
  private static final Reference BRANCH_1 = Branch.of("branch1", "cafebabe");
  private static final Reference BRANCH_2 = Branch.of("branch2", "deadbeef");

  @Test
  void testCommit() throws IOException {
    CommitEvent event =
        ImmutableCommitEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .reference(BRANCH_1)
            .hashBefore("hash1")
            .hashAfter("hash2")
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .eventInitiator("Alice")
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .committer("committer")
                    .message("message")
                    .commitTime(Instant.parse("2024-09-26T11:11:11Z"))
                    .authorTime(Instant.parse("2024-09-26T22:22:22Z"))
                    .build())
            .build();
    assertThat(event.getType()).isEqualTo(EventType.COMMIT);
    testSerde(event, "commit.json", CommitEvent.class);
  }

  @Test
  void merge() throws IOException {
    MergeEvent event =
        ImmutableMergeEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .sourceReference(BRANCH_1)
            .targetReference(BRANCH_2)
            .hashBefore("hash1")
            .hashAfter("hash2")
            .sourceHash("hash3")
            .repositoryId("repo1")
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .eventInitiator("Alice")
            .commonAncestorHash("hash0")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.MERGE);
    testSerde(event, "merge.json", MergeEvent.class);
  }

  @Test
  void transplant() throws IOException {
    TransplantEvent event =
        ImmutableTransplantEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .targetReference(BRANCH_1)
            .hashBefore("hash1")
            .hashAfter("hash2")
            .commitCount(3)
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .eventInitiator("Alice")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.TRANSPLANT);
    testSerde(event, "transplant.json", TransplantEvent.class);
  }

  @Test
  void referenceCreated() throws IOException {
    ReferenceCreatedEvent event =
        ImmutableReferenceCreatedEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .reference(BRANCH_1)
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .eventInitiator("Alice")
            .hashAfter("hash2")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.REFERENCE_CREATED);
    testSerde(event, "reference-created.json", ReferenceCreatedEvent.class);
  }

  @Test
  void referenceUpdated() throws IOException {
    ReferenceUpdatedEvent event =
        ImmutableReferenceUpdatedEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .reference(BRANCH_1)
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .eventInitiator("Alice")
            .hashBefore("hash1")
            .hashAfter("hash2")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.REFERENCE_UPDATED);
    testSerde(event, "reference-updated.json", ReferenceUpdatedEvent.class);
  }

  @Test
  void referenceDeleted() throws IOException {
    ReferenceDeletedEvent event =
        ImmutableReferenceDeletedEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .reference(BRANCH_1)
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .eventInitiator("Alice")
            .hashBefore("hash1")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.REFERENCE_DELETED);
    testSerde(event, "reference-deleted.json", ReferenceDeletedEvent.class);
  }

  @Test
  void contentStored() throws IOException {
    ContentStoredEvent event =
        ImmutableContentStoredEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .reference(BRANCH_1)
            .hash("hash1")
            .contentKey(ContentKey.of("ns", "table1"))
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .commitCreationTimestamp(Instant.parse("2024-09-26T11:11:11Z"))
            .eventInitiator("Alice")
            .content(IcebergTable.of("metadataLocation", 1L, 2, 3, 4, "id"))
            .build();
    assertThat(event.getType()).isEqualTo(EventType.CONTENT_STORED);
    testSerde(event, "content-stored.json", ContentStoredEvent.class);
  }

  @Test
  void contentRemoved() throws IOException {
    ContentRemovedEvent event =
        ImmutableContentRemovedEvent.builder()
            .id(ID)
            .repositoryId("repo1")
            .reference(BRANCH_1)
            .hash("hash1")
            .contentKey(ContentKey.of("ns", "table1"))
            .eventCreationTimestamp(Instant.parse("2024-09-26T00:00:00Z"))
            .commitCreationTimestamp(Instant.parse("2024-09-26T11:11:11Z"))
            .eventInitiator("Alice")
            .build();
    assertThat(event.getType()).isEqualTo(EventType.CONTENT_REMOVED);
    testSerde(event, "content-removed.json", ContentRemovedEvent.class);
  }

  private void testSerde(Event event, String resource, Class<? extends Event> eventClass)
      throws IOException {
    String serialized = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(event);
    assertThat(MAPPER.readValue(serialized, MAP_TYPE))
        .isEqualTo(MAPPER.readValue(getClass().getResourceAsStream(resource), MAP_TYPE));
    Event deserialized = MAPPER.readValue(serialized, eventClass);
    assertThat(deserialized).isEqualTo(event);
  }
}

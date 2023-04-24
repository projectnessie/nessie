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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TestJsonSerde {

  static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .registerModule(new Jdk8Module())
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES)
          .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
          .enable(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE);

  @Test
  void commit() throws Exception {
    CommitEvent event =
        ImmutableCommitEvent.builder()
            .sourceReference("branch1")
            .targetBranch("branch2")
            .hashBefore("hash1")
            .hashAfter("hash2")
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .commitMeta(
                ImmutableCommitMeta.builder()
                    .commitTime(Instant.now())
                    .committer("committer")
                    .message("message")
                    .authorTime(Instant.now())
                    .build())
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void merge() throws Exception {
    MergeEvent event =
        ImmutableMergeEvent.builder()
            .sourceReference("branch1")
            .targetBranch("branch2")
            .hashBefore("hash1")
            .hashAfter("hash2")
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .commonAncestorHash("hash0")
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void transplant() throws Exception {
    TransplantEvent event =
        ImmutableTransplantEvent.builder()
            .sourceReference("branch1")
            .targetBranch("branch2")
            .hashBefore("hash1")
            .hashAfter("hash2")
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void referenceCreated() throws Exception {
    ReferenceCreatedEvent event =
        ImmutableReferenceCreatedEvent.builder()
            .referenceName("ref1")
            .referenceType(ReferenceType.BRANCH)
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .hashAfter("hash2")
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void referenceUpdated() throws Exception {
    ReferenceUpdatedEvent event =
        ImmutableReferenceUpdatedEvent.builder()
            .referenceName("ref1")
            .referenceType(ReferenceType.BRANCH)
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .hashBefore("hash1")
            .hashAfter("hash2")
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void referenceDeleted() throws Exception {
    ReferenceDeletedEvent event =
        ImmutableReferenceDeletedEvent.builder()
            .referenceName("ref1")
            .referenceType(ReferenceType.BRANCH)
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .hashBefore("hash1")
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void contentStored() throws Exception {
    ContentStoredEvent event =
        ImmutableContentStoredEvent.builder()
            .branch("branch1")
            .hash("hash1")
            .contentKey(ContentKey.of("ns", "table1"))
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .content(
                ImmutableIcebergTable.builder()
                    .metadataLocation("metadataLocation")
                    .id(UUID.randomUUID())
                    .snapshotId(1L)
                    .schemaId(2)
                    .specId(3)
                    .sortOrderId(4)
                    .build())
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void contentRemoved() throws Exception {
    ContentRemovedEvent event =
        ImmutableContentRemovedEvent.builder()
            .branch("branch1")
            .hash("hash1")
            .contentKey(ContentKey.of("ns", "table1"))
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .sentBy("Nessie")
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void icebergTable() throws Exception {
    IcebergTable content =
        ImmutableIcebergTable.builder()
            .id(UUID.randomUUID())
            .metadataLocation("metadataLocation")
            .snapshotId(1L)
            .schemaId(2)
            .specId(3)
            .sortOrderId(4)
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void deltaLakeTable() throws Exception {
    DeltaLakeTable content =
        ImmutableDeltaLakeTable.builder()
            .id(UUID.randomUUID())
            .addCheckpointLocationHistory("checkpoint")
            .addMetadataLocationHistory("metadata")
            .lastCheckpoint("lastCheckpoint")
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void icebergView() throws Exception {
    IcebergView content =
        ImmutableIcebergView.builder()
            .id(UUID.randomUUID())
            .metadataLocation("metadataLocation")
            .versionId(1L)
            .schemaId(2)
            .sqlText("sqlText")
            .dialect("dialect")
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void customContent() throws Exception {
    CustomContent content =
        ImmutableCustomContent.builder()
            .id(UUID.randomUUID())
            .customType("customType")
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  private Object deserialize(String json, Class<?> clazz) throws JsonProcessingException {
    return MAPPER.readValue(json, clazz);
  }

  private String serialize(Object event) throws JsonProcessingException {
    return MAPPER.writeValueAsString(event);
  }
}

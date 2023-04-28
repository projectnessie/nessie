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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableMap;
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
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
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
            .commonAncestorHash("hash0")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
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
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void referenceCreated() throws Exception {
    ReferenceCreatedEvent event =
        ImmutableReferenceCreatedEvent.builder()
            .referenceName("ref1")
            .fullReferenceName("fullRef1")
            .referenceType(ReferenceEvent.BRANCH)
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .hashAfter("hash2")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void referenceUpdated() throws Exception {
    ReferenceUpdatedEvent event =
        ImmutableReferenceUpdatedEvent.builder()
            .referenceName("ref1")
            .fullReferenceName("fullRef1")
            .referenceType(ReferenceEvent.BRANCH)
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .hashBefore("hash1")
            .hashAfter("hash2")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void referenceDeleted() throws Exception {
    ReferenceDeletedEvent event =
        ImmutableReferenceDeletedEvent.builder()
            .referenceName("ref1")
            .fullReferenceName("fullRef1")
            .referenceType(ReferenceEvent.BRANCH)
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .createdBy("Alice")
            .hashBefore("hash1")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
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
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .content(
                ImmutableIcebergTable.builder()
                    .metadataLocation("metadataLocation")
                    .id("id")
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
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void icebergTable() throws Exception {
    IcebergTable content =
        ImmutableIcebergTable.builder()
            .id("id")
            .metadataLocation("metadataLocation")
            .snapshotId(1L)
            .schemaId(2)
            .specId(3)
            .sortOrderId(4)
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void deltaLakeTable() throws Exception {
    DeltaLakeTable content =
        ImmutableDeltaLakeTable.builder()
            .id("id")
            .addCheckpointLocationHistory("checkpoint")
            .addMetadataLocationHistory("metadata")
            .lastCheckpoint("lastCheckpoint")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void icebergView() throws Exception {
    IcebergView content =
        ImmutableIcebergView.builder()
            .id("id")
            .metadataLocation("metadataLocation")
            .versionId(1L)
            .schemaId(2)
            .sqlText("sqlText")
            .dialect("dialect")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void namespace() throws Exception {
    Namespace content =
        ImmutableNamespace.builder()
            .id("id")
            .addElement("level1")
            .addElement("level2")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void customEvent() throws Exception {
    CustomEvent event =
        ImmutableCustomEvent.builder()
            .id(UUID.fromString("7385d1e6-3deb-440b-9008-a383e2de6e6c"))
            .customType("weird")
            .repositoryId("repo1")
            .createdAt(Instant.parse("2023-04-25T13:02:05Z"))
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(event), Event.class)).isEqualTo(event);
  }

  @Test
  void customEventSerialization() throws Exception {
    CustomEvent event =
        ImmutableCustomEvent.builder()
            .id(UUID.fromString("7385d1e6-3deb-440b-9008-a383e2de6e6c"))
            .customType("weird")
            .repositoryId("repo1")
            .createdAt(Instant.parse("2023-04-25T13:02:05Z"))
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(serialize(event))
        .isEqualTo(
            "{"
                + "\"id\":\"7385d1e6-3deb-440b-9008-a383e2de6e6c\","
                + "\"type\":\"weird\","
                + "\"repositoryId\":\"repo1\","
                + "\"createdAt\":\"2023-04-25T13:02:05Z\","
                + "\"string\":\"foo\","
                + "\"number\":123,"
                + "\"boolean\":true,"
                + "\"complex\":{"
                + "\"string\":\"foo\","
                + "\"number\":123,"
                + "\"boolean\":true"
                + "}"
                + "}");
  }

  @Test
  void customContent() throws Exception {
    CustomContent content =
        ImmutableCustomContent.builder()
            .id("id")
            .customType("customType")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(deserialize(serialize(content), Content.class)).isEqualTo(content);
  }

  @Test
  void customContentSerialization() throws Exception {
    CustomContent content =
        ImmutableCustomContent.builder()
            .id("id")
            .customType("weird")
            .putProperty("string", "foo")
            .putProperty("number", 123)
            .putProperty("boolean", true)
            .putProperty(
                "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
            .build();
    assertThat(serialize(content))
        .isEqualTo(
            "{"
                + "\"id\":\"id\","
                + "\"type\":\"weird\","
                + "\"string\":\"foo\","
                + "\"number\":123,"
                + "\"boolean\":true,"
                + "\"complex\":{"
                + "\"string\":\"foo\","
                + "\"number\":123,"
                + "\"boolean\":true"
                + "}"
                + "}");
  }

  @Test
  void unknownEventDeserialization() throws Exception {
    assertThat(
            deserialize(
                "{"
                    + "\"id\":\"7385d1e6-3deb-440b-9008-a383e2de6e6c\","
                    + "\"type\":\"weird\","
                    + "\"repositoryId\":\"repo1\","
                    + "\"createdAt\":\"2023-04-25T13:02:05Z\","
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true,"
                    + "\"complex\":{"
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true"
                    + "}"
                    + "}",
                Event.class))
        .isEqualTo(
            ImmutableCustomEvent.builder()
                .id(UUID.fromString("7385d1e6-3deb-440b-9008-a383e2de6e6c"))
                .customType("weird")
                .repositoryId("repo1")
                .createdAt(Instant.parse("2023-04-25T13:02:05Z"))
                .putProperty("string", "foo")
                .putProperty("number", 123)
                .putProperty("boolean", true)
                .putProperty(
                    "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
                .build());
  }

  @Test
  void unknownContentDeserialization() throws Exception {
    assertThat(
            deserialize(
                "{"
                    + "\"id\":\"id\","
                    + "\"type\":\"weird\","
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true,"
                    + "\"complex\":{"
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true"
                    + "}"
                    + "}",
                Content.class))
        .isEqualTo(
            ImmutableCustomContent.builder()
                .id("id")
                .customType("weird")
                .putProperty("string", "foo")
                .putProperty("number", 123)
                .putProperty("boolean", true)
                .putProperty(
                    "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
                .build());
  }

  @Test
  void deserializeKnownEventTypeToCustomSubtype() throws Exception {
    assertThat(
            deserialize(
                "{"
                    + "\"id\":\"7385d1e6-3deb-440b-9008-a383e2de6e6c\","
                    + "\"type\":\"REFERENCE_CREATED\","
                    + "\"repositoryId\":\"repo1\","
                    + "\"createdAt\":\"2023-04-25T13:02:05Z\","
                    + "\"hashAfter\":\"1234\","
                    + "\"referenceName\":\"ref1\","
                    + "\"fullReferenceName\":\"refs/heads/ref1\","
                    + "\"referenceType\":\"BRANCH\","
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true,"
                    + "\"complex\":{"
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true"
                    + "}"
                    + "}",
                CustomEvent.class))
        .isEqualTo(
            ImmutableCustomEvent.builder()
                .id(UUID.fromString("7385d1e6-3deb-440b-9008-a383e2de6e6c"))
                .customType("REFERENCE_CREATED")
                .repositoryId("repo1")
                .createdAt(Instant.parse("2023-04-25T13:02:05Z"))
                .putProperty("hashAfter", "1234")
                .putProperty("referenceName", "ref1")
                .putProperty("fullReferenceName", "refs/heads/ref1")
                .putProperty("referenceType", "BRANCH")
                .putProperty("string", "foo")
                .putProperty("number", 123)
                .putProperty("boolean", true)
                .putProperty(
                    "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
                .build());
  }

  @Test
  void deserializeKnownContentTypeToCustomSubtype() throws Exception {
    assertThat(
            deserialize(
                "{"
                    + "\"id\":\"id\","
                    + "\"type\":\"ICEBERG_TABLE\","
                    + "\"metadataLocation\":\"location\","
                    + "\"snapshotId\":1,"
                    + "\"schemaId\":2,"
                    + "\"specId\":3,"
                    + "\"sortOrderId\":4,"
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true,"
                    + "\"complex\":{"
                    + "\"string\":\"foo\","
                    + "\"number\":123,"
                    + "\"boolean\":true"
                    + "}"
                    + "}",
                CustomContent.class))
        .isEqualTo(
            ImmutableCustomContent.builder()
                .id("id")
                .customType("ICEBERG_TABLE")
                .putProperty("metadataLocation", "location")
                .putProperty("snapshotId", 1)
                .putProperty("schemaId", 2)
                .putProperty("specId", 3)
                .putProperty("sortOrderId", 4)
                .putProperty("string", "foo")
                .putProperty("number", 123)
                .putProperty("boolean", true)
                .putProperty(
                    "complex", ImmutableMap.of("string", "foo", "number", 123, "boolean", true))
                .build());
  }

  @Test
  void deserializeEventToIncompatibleSubtype() {
    assertThatThrownBy(
            () ->
                deserialize(
                    "{"
                        + "\"id\":\"7385d1e6-3deb-440b-9008-a383e2de6e6c\","
                        + "\"type\":\"REFERENCE_CREATED\","
                        + "\"repositoryId\":\"repo1\","
                        + "\"createdAt\":\"2023-04-25T13:02:05Z\","
                        + "\"hashAfter\":\"1234\","
                        + "\"referenceName\":\"ref1\","
                        + "\"fullReferenceName\":\"refs/heads/ref1\","
                        + "\"referenceType\":\"BRANCH\""
                        + "}",
                    ReferenceDeletedEvent.class))
        .isInstanceOf(JsonMappingException.class)
        .hasMessageContaining(
            "Type id REFERENCE_CREATED is not convertible to interface org.projectnessie.events.api.ReferenceDeletedEvent");
  }

  @Test
  void deserializeContentToIncompatibleSubtype() {
    assertThatThrownBy(
            () ->
                deserialize(
                    "{"
                        + "\"id\":\"id\","
                        + "\"type\":\"ICEBERG_TABLE\","
                        + "\"metadataLocation\":\"location\","
                        + "\"snapshotId\":1,"
                        + "\"schemaId\":2,"
                        + "\"specId\":3,"
                        + "\"sortOrderId\":4"
                        + "}",
                    IcebergView.class))
        .isInstanceOf(JsonMappingException.class)
        .hasMessageContaining(
            "Type id ICEBERG_TABLE is not convertible to interface org.projectnessie.events.api.IcebergView");
  }

  private Object deserialize(String json, Class<?> clazz) throws JsonProcessingException {
    return MAPPER.readValue(json, clazz);
  }

  private String serialize(Object event) throws JsonProcessingException {
    return MAPPER.writeValueAsString(event);
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.projectnessie.events.api.TestEventType.committingAttributes;
import static org.projectnessie.events.api.TestEventType.contentAttributes;
import static org.projectnessie.events.api.TestEventType.refAttributes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TestJsonSerde {

  static final ObjectMapper MAPPER =
      new ObjectMapper().registerModule(new JavaTimeModule()).registerModule(new Jdk8Module());

  @Test
  void commit() throws Exception {
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
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void merge() throws Exception {
    MergeEvent event =
        committingAttributes(MergeEvent.builder()).commonAncestorHash("hash0").build();
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void transplant() throws Exception {
    TransplantEvent event = committingAttributes(TransplantEvent.builder()).build();
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void referenceCreated() throws Exception {
    ReferenceCreatedEvent event =
        refAttributes(ReferenceCreatedEvent.builder()).hashAfter("hash2").build();
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void referenceUpdated() throws Exception {
    ReferenceUpdatedEvent event =
        refAttributes(ReferenceUpdatedEvent.builder())
            .hashBefore("hash1")
            .hashAfter("hash2")
            .build();
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void referenceDeleted() throws Exception {
    ReferenceDeletedEvent event =
        refAttributes(ReferenceDeletedEvent.builder()).hashBefore("hash1").build();
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void contentStored() throws Exception {
    ContentStoredEvent event =
        contentAttributes(ContentStoredEvent.builder())
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
    assertEquals(event, deserialize(serialize(event), Event.class));
  }

  @Test
  void contentRemoved() throws Exception {
    ContentRemovedEvent event = contentAttributes(ContentRemovedEvent.builder()).build();
    assertEquals(event, deserialize(serialize(event), Event.class));
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
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertEquals(content, deserialize(serialize(content), Content.class));
  }

  @Test
  void deltaLakeTable() throws Exception {
    DeltaLakeTable content =
        ImmutableDeltaLakeTable.builder()
            .id("id")
            .addCheckpointLocationHistory("checkpoint")
            .addMetadataLocationHistory("metadata")
            .lastCheckpoint("lastCheckpoint")
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertEquals(content, deserialize(serialize(content), Content.class));
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
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertEquals(content, deserialize(serialize(content), Content.class));
  }

  @Test
  void customContent() throws Exception {
    CustomContent content =
        ImmutableCustomContent.builder()
            .id("id")
            .putAttribute("string", "foo")
            .putAttribute("number", 123)
            .build();
    assertEquals(content, deserialize(serialize(content), Content.class));
  }

  private Object deserialize(String json, Class<?> clazz) throws JsonProcessingException {
    return MAPPER.readValue(json, clazz);
  }

  private String serialize(Object event) throws JsonProcessingException {
    return MAPPER.writeValueAsString(event);
  }
}

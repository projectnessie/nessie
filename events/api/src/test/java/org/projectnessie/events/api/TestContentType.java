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

import org.junit.jupiter.api.Test;

class TestContentType {

  @Test
  void namespace() {
    Namespace content = ImmutableNamespace.builder().id("id").addElements("name").build();
    assertEquals(ContentType.NAMESPACE, content.getType());
  }

  @Test
  void icebergTable() {
    IcebergTable content =
        ImmutableIcebergTable.builder()
            .id("id")
            .metadataLocation("metadataLocation")
            .snapshotId(1L)
            .schemaId(2)
            .specId(3)
            .sortOrderId(4)
            .build();
    assertEquals(ContentType.ICEBERG_TABLE, content.getType());
  }

  @Test
  void deltaLakeTable() {
    DeltaLakeTable content =
        ImmutableDeltaLakeTable.builder().id("id").lastCheckpoint("lastCheckpoint").build();
    assertEquals(ContentType.DELTA_LAKE_TABLE, content.getType());
  }

  @Test
  void icebergView() {
    IcebergView content =
        ImmutableIcebergView.builder()
            .id("id")
            .metadataLocation("metadataLocation")
            .versionId(1L)
            .schemaId(2)
            .sqlText("sqlText")
            .dialect("dialect")
            .build();
    assertEquals(ContentType.ICEBERG_VIEW, content.getType());
  }

  @Test
  void custom() {
    CustomContent content =
        ImmutableCustomContent.builder().id("id").customType("customType").build();
    assertEquals(ContentType.CUSTOM, content.getType());
  }
}

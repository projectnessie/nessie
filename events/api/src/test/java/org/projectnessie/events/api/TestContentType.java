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

import org.junit.jupiter.api.Test;

class TestContentType {

  @Test
  void namespace() {
    Namespace content = ImmutableNamespace.builder().id("id").addElements("name").build();
    assertThat(content.getType()).isEqualTo(ContentType.NAMESPACE);
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
    assertThat(content.getType()).isEqualTo(ContentType.ICEBERG_TABLE);
  }

  @Test
  void deltaLakeTable() {
    DeltaLakeTable content =
        ImmutableDeltaLakeTable.builder().id("id").lastCheckpoint("lastCheckpoint").build();
    assertThat(content.getType()).isEqualTo(ContentType.DELTA_LAKE_TABLE);
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
    assertThat(content.getType()).isEqualTo(ContentType.ICEBERG_VIEW);
  }

  @Test
  void udf() {
    UDF content = ImmutableUDF.builder().id("id").sqlText("sqlText").dialect("dialect").build();
    assertThat(content.getType()).isEqualTo(ContentType.UDF);
  }

  @Test
  void generic() {
    GenericContent content =
        ImmutableGenericContent.builder().id("id").genericType("genericType").build();
    assertThat(content.getType()).isEqualTo(ContentType.GENERIC);
  }
}

/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.IcebergContent.CURRENT_SCHEMA_ID;
import static org.projectnessie.model.IcebergContent.CURRENT_SNAPSHOT_ID;
import static org.projectnessie.model.IcebergContent.DEFAULT_SORT_ORDER_ID;
import static org.projectnessie.model.IcebergContent.DEFAULT_SPEC_ID;
import static org.projectnessie.model.IcebergContent.ICEBERG_METADATA_VARIANT;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestIcebergTable {

  @ParameterizedTest
  @MethodSource("validMetadata")
  public void validMetadata(
      ObjectNode metadata, long snapshotId, int schemaId, int sortOrderId, int specId) {
    IcebergTable view = IcebergTable.of(metadata, "metadataLocation", "id");
    assertThat(view)
        .extracting(
            IcebergTable::getMetadataLocation,
            IcebergTable::getId,
            IcebergTable::getSnapshotId,
            IcebergTable::getSchemaId,
            IcebergTable::getSortOrderId,
            IcebergTable::getSpecId,
            IcebergTable::getMetadata)
        .containsExactly(
            "metadataLocation",
            "id",
            snapshotId,
            schemaId,
            sortOrderId,
            specId,
            GenericMetadata.of(ICEBERG_METADATA_VARIANT, metadata));
  }

  static Stream<Arguments> validMetadata() {
    return Stream.of(
        arguments(metadata(), -1L, 0, 0, 0),
        arguments(
            metadata()
                .put(CURRENT_SCHEMA_ID, 66)
                .put(DEFAULT_SORT_ORDER_ID, 123)
                .put(DEFAULT_SPEC_ID, 345),
            -1L,
            66,
            123,
            345),
        arguments(
            metadata()
                .put(CURRENT_SNAPSHOT_ID, 42L)
                .put(DEFAULT_SORT_ORDER_ID, 123)
                .put(DEFAULT_SPEC_ID, 345),
            42L,
            0,
            123,
            345),
        arguments(
            metadata()
                .put(CURRENT_SNAPSHOT_ID, 42L)
                .put(CURRENT_SCHEMA_ID, 66)
                .put(DEFAULT_SPEC_ID, 345),
            42L,
            66,
            0,
            345),
        arguments(
            metadata()
                .put(CURRENT_SNAPSHOT_ID, 42L)
                .put(CURRENT_SCHEMA_ID, 66)
                .put(DEFAULT_SORT_ORDER_ID, 123),
            42L,
            66,
            123,
            0));
  }

  private static ObjectNode metadata() {
    return JsonNodeFactory.instance.objectNode();
  }
}

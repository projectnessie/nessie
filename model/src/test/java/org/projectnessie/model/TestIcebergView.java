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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.model.IcebergContent.CURRENT_VERSION_ID;
import static org.projectnessie.model.IcebergContent.ICEBERG_METADATA_VARIANT;
import static org.projectnessie.model.IcebergContent.SCHEMA;
import static org.projectnessie.model.IcebergContent.SCHEMA_ID;
import static org.projectnessie.model.IcebergContent.SQL;
import static org.projectnessie.model.IcebergContent.VERSION_ID;
import static org.projectnessie.model.IcebergContent.VIEW_DEFINITION;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestIcebergView {
  @ParameterizedTest
  @MethodSource("nonExistingFields")
  public void nonExistingFields(JsonNode metadata) {
    assertThatThrownBy(() -> IcebergView.of(metadata, "metadataLocation", "id"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void validMetadata() {
    ObjectNode metadata =
        versions(
            42,
            array -> {
              ObjectNode version = array.addObject().put(VERSION_ID, 42);
              ObjectNode viewDefinition = version.putObject(VIEW_DEFINITION);
              viewDefinition.put(SQL, "SELECT bar FROM baz");
              ObjectNode schema = viewDefinition.putObject(SCHEMA);
              schema.put(SCHEMA_ID, 66);
            });

    IcebergView view = IcebergView.of(metadata, "metadataLocation", "id");
    assertThat(view)
        .extracting(
            IcebergView::getMetadataLocation,
            IcebergView::getId,
            IcebergView::getVersionId,
            IcebergView::getSchemaId,
            IcebergView::getDialect,
            IcebergView::getSqlText,
            IcebergView::getMetadata)
        .containsExactly(
            "metadataLocation",
            "id",
            42,
            66,
            "",
            "SELECT bar FROM baz",
            GenericMetadata.of(ICEBERG_METADATA_VARIANT, metadata));
  }

  static Stream<JsonNode> nonExistingFields() {
    return Stream.of(
        // empty metadata, no CURRENT_VERSION_ID
        metadata(),
        // metadata, empty version
        versions(-1, ArrayNode::addObject),
        // metadata, CURRENT_VERSION_ID + VERSION_ID, missing VIEW_DEFINITION
        versions(42, array -> array.addObject().put(VERSION_ID, 42)),
        // metadata, CURRENT_VERSION_ID + VERSION_ID, VIEW_DEFINITION, missing SQL
        versions(
            42,
            array -> {
              ObjectNode version = array.addObject().put(VERSION_ID, 42);
              ObjectNode viewDefinition = version.putObject(VIEW_DEFINITION);
            }),
        // metadata, CURRENT_VERSION_ID + VERSION_ID, VIEW_DEFINITION, SQL, missing SCHEMA
        versions(
            42,
            array -> {
              ObjectNode version = array.addObject().put(VERSION_ID, 42);
              ObjectNode viewDefinition = version.putObject(VIEW_DEFINITION);
              viewDefinition.put(SQL, "SELECT foo FROM bar");
            }),
        // metadata, CURRENT_VERSION_ID + VERSION_ID, VIEW_DEFINITION, SQL, SCHEMA, missing
        // SCHEMA_ID
        versions(
            42,
            array -> {
              ObjectNode version = array.addObject().put(VERSION_ID, 42);
              ObjectNode viewDefinition = version.putObject(VIEW_DEFINITION);
              viewDefinition.put(SQL, "SELECT foo FROM bar");
              ObjectNode schema = viewDefinition.putObject(SCHEMA);
            }));
  }

  private static ObjectNode versions(int currentVersionId, Consumer<ArrayNode> arrayNodeConsumer) {
    return versions(metadata().put(CURRENT_VERSION_ID, currentVersionId), arrayNodeConsumer);
  }

  private static ObjectNode versions(ObjectNode container, Consumer<ArrayNode> arrayNodeConsumer) {
    ArrayNode array = container.putArray(IcebergContent.VERSIONS);
    arrayNodeConsumer.accept(array);
    return container;
  }

  private static ObjectNode metadata() {
    return JsonNodeFactory.instance.objectNode();
  }
}

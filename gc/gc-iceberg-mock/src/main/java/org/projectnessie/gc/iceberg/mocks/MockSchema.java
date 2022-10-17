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
package org.projectnessie.gc.iceberg.mocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockSchema {

  public static final MockSchema DEFAULT_EMPTY = MockTableMetadata.empty().schema(0);

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("type", type()).put("schema-id", schemaId());
    fields(node.putArray("fields"));
    return node;
  }

  @Value.Default
  public List<MockField> fields() {
    return Collections.singletonList(ImmutableMockField.builder().build());
  }

  private void fields(ArrayNode fields) {
    for (MockField field : fields()) {
      fields.add(field.jsonNode());
    }
  }

  @Value.Default
  public String type() {
    return "struct";
  }

  @Value.Default
  public int schemaId() {
    return 0;
  }

  public Schema toSchema() {
    return new Schema(
        schemaId(), fields().stream().map(MockField::toNestedField).collect(Collectors.toList()));
  }
}

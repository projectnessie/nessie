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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockPartitionSpec {

  public static final MockPartitionSpec DEFAULT_EMPTY = MockTableMetadata.empty().partitionSpec(0);

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("spec-id", specId());
    fields(node.putArray("fields"));
    return node;
  }

  @Value.Default
  public List<MockPartitionSpecField> fields() {
    return Collections.singletonList(ImmutableMockPartitionSpecField.builder().build());
  }

  private void fields(ArrayNode fields) {
    for (MockPartitionSpecField field : fields()) {
      fields.add(field.jsonNode());
    }
  }

  @Value.Default
  public int specId() {
    return 0;
  }

  public PartitionSpec toPartitionSpec(Schema schema) {
    PartitionSpec.Builder partitionSpecBuilder =
        PartitionSpec.builderFor(schema).withSpecId(specId());
    fields().forEach(f -> partitionSpecBuilder.identity(f.name()));
    return partitionSpecBuilder.build();
  }
}

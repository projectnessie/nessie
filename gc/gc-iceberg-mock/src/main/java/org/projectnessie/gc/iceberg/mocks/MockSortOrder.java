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
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockSortOrder {

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("order-id", orderId());
    fields(node.putArray("fields"));
    return node;
  }

  @Value.Default
  public List<MockSortOrderField> fields() {
    return Collections.singletonList(ImmutableMockSortOrderField.builder().build());
  }

  private void fields(ArrayNode fields) {
    for (MockSortOrderField field : fields()) {
      fields.add(field.jsonNode());
    }
  }

  @Value.Default
  public int orderId() {
    return 1;
  }
}

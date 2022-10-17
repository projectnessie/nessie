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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockPartitionSpecField {

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("name", name())
        .put("transform", transform())
        .put("source-id", sourceId())
        .put("field-id", fieldId());
    return node;
  }

  @Value.Default
  public String name() {
    return "name";
  }

  @Value.Default
  public String transform() {
    return "identity";
  }

  @Value.Default
  public int sourceId() {
    return 0;
  }

  @Value.Default
  public int fieldId() {
    return 0;
  }
}

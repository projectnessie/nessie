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
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockField {

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("id", id()).put("name", name()).put("required", required()).put("type", type());
    return node;
  }

  @Value.Default
  public int id() {
    return 0;
  }

  @Value.Default
  public String name() {
    return "name";
  }

  @Value.Default
  public boolean required() {
    return false;
  }

  @Value.Default
  public String type() {
    return "int";
  }

  @Value.Auxiliary
  public NestedField toNestedField() {
    return NestedField.builder()
        .withId(id())
        .isOptional(!required())
        .withName(name())
        .ofType(Types.fromPrimitiveString(type()))
        .build();
  }
}

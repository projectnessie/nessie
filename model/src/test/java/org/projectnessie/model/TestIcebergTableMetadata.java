/*
 * Copyright (C) 2020 Dremio
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.iceberg.IcebergSchema;

public class TestIcebergTableMetadata {
  @Test
  public void schemaObjectType() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    JsonNode someGenericJson =
        mapper.readTree(
            "{\n"
                + "  \"foo\": \"bar\",\n"
                + "  \"someArray\": [1, 2, 3, 4, 5],\n"
                + "  \"nested\": {\n"
                + "    \"abc\": \"def\"\n"
                + "  }\n"
                + "}");

    IcebergSchema schema =
        IcebergSchema.builder()
            .schemaId(42)
            .identifierFieldIds(1, 3, 5)
            .struct(someGenericJson)
            .build();
    String schemaAsJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(schema);

    IcebergSchema deserialized = mapper.readValue(schemaAsJson, IcebergSchema.class);
    assertThat(deserialized).isEqualTo(schema);
  }
}

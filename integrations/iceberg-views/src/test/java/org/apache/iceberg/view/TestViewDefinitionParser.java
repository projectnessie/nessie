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
package org.apache.iceberg.view;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestViewDefinitionParser {

  @Test
  public void testBasic() throws IOException {
    checkViewDefinitionSerialization(
        "SELECT 'foo' foo",
        new Schema(optional(1, "foo", Types.StringType.get())),
        "cat",
        Arrays.asList("part1", "part2"));
  }

  @Test
  public void testEmptyCatalogNamespace() throws IOException {
    checkViewDefinitionSerialization(
        "SELECT 1 intcol, 'str' strcol",
        new Schema(
            optional(1, "intcol", Types.IntegerType.get()),
            optional(2, "strcol", Types.StringType.get())),
        "",
        Collections.emptyList());
  }

  private void checkViewDefinitionSerialization(
      String sql, Schema schema, String sessionCatalog, List<String> sessionNamespace)
      throws IOException {
    ViewDefinition expected = ViewDefinition.of(sql, schema, sessionCatalog, sessionNamespace);

    Writer jsonWriter = new StringWriter();
    JsonGenerator generator = new JsonFactory().createGenerator(jsonWriter);
    ViewDefinitionParser.toJson(expected, generator);
    generator.flush();

    ViewDefinition actual =
        ViewDefinitionParser.fromJson(
            JsonUtil.mapper().readValue(jsonWriter.toString(), JsonNode.class));

    assertThat(actual).isEqualTo(expected);
  }
}

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
package org.projectnessie.model.metatdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentMetadata;

@ExtendWith(SoftAssertionsExtension.class)
public class TestContentMetadata {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<JsonNode> unknownMetadataObject() {
    return Stream.of(
        new ObjectNode(JsonNodeFactory.instance)
            .put("variant", "something-unknown")
            .put("a", "b")
            .put("c", "d"),
        new ObjectNode(JsonNodeFactory.instance)
            .put("variant", "something-unknown")
            .put("a", "b")
            .set("arr", new ArrayNode(JsonNodeFactory.instance).add(42).add("foo").add(true)),
        new ObjectNode(JsonNodeFactory.instance)
            .put("variant", "something-unknown")
            .put("a", "b")
            .set(
                "c",
                new ObjectNode(JsonNodeFactory.instance)
                    .put("i1", "v1")
                    .put("i2", "v2")
                    .set("i3", new ObjectNode(JsonNodeFactory.instance).put("x1", "y1"))),
        new ObjectNode(JsonNodeFactory.instance)
            .put("a", "b")
            .put("c", "d")
            .put("variant", "something-unknown"));
  }

  @ParameterizedTest
  @MethodSource
  void unknownMetadataObject(JsonNode candidate) throws Exception {
    String jsonString = MAPPER.writeValueAsString(candidate);
    ContentMetadata meta = MAPPER.readValue(jsonString, ContentMetadata.class);

    soft.assertThat(meta.getVariant()).isEqualTo("something-unknown");

    String serialized = MAPPER.writeValueAsString(meta);

    JsonNode serializedAsJsonNode = MAPPER.readValue(serialized, JsonNode.class);
    soft.assertThat(serializedAsJsonNode).isEqualTo(candidate);
  }
}

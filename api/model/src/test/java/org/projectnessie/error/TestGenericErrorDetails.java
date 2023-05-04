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
package org.projectnessie.error;

import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;

@ExtendWith(SoftAssertionsExtension.class)
public class TestGenericErrorDetails {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @InjectSoftAssertions protected SoftAssertions soft;

  static Stream<JsonNode> unknownErrorType() {
    return Stream.of(
        new ObjectNode(JsonNodeFactory.instance)
            .put("type", "something-unknown")
            .put("id", "123")
            .put("a", "b")
            .put("c", "d"),
        new ObjectNode(JsonNodeFactory.instance)
            .put("type", "something-unknown")
            .put("a", "b")
            .set("arr", new ArrayNode(JsonNodeFactory.instance).add(42).add("foo").add(true)),
        new ObjectNode(JsonNodeFactory.instance)
            .put("type", "something-unknown")
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
            .put("type", "something-unknown"));
  }

  @ParameterizedTest
  @MethodSource
  void unknownErrorType(JsonNode candidate) throws Exception {
    String jsonString = MAPPER.writeValueAsString(candidate);
    NessieErrorDetails meta = MAPPER.readValue(jsonString, NessieErrorDetails.class);

    soft.assertThat(meta.getType()).isEqualTo("something-unknown");

    String serialized = MAPPER.writeValueAsString(meta);

    JsonNode serializedAsJsonNode = MAPPER.readValue(serialized, JsonNode.class);
    soft.assertThat(serializedAsJsonNode).isEqualTo(candidate);
  }

  static Stream<NessieErrorDetails> knownErrorDetailsAsUnknown() {
    return Stream.of(
        ReferenceConflicts.referenceConflicts(
            Conflict.conflict(
                Conflict.ConflictType.UNEXPECTED_HASH, ContentKey.of("a", "b"), "hello")),
        ContentKeyErrorDetails.contentKeyErrorDetails(ContentKey.of("a", "b")));
  }

  @ParameterizedTest
  @MethodSource
  void knownErrorDetailsAsUnknown(NessieErrorDetails errorDetails) throws Exception {
    TokenBuffer tokenBuffer = new TokenBuffer(MAPPER, false);
    MAPPER.writeValue(tokenBuffer, errorDetails);
    JsonNode jsonNode = tokenBuffer.asParser().readValueAsTree();

    String jsonString = MAPPER.writeValueAsString(errorDetails);
    NessieErrorDetails meta = MAPPER.readValue(jsonString, NessieErrorDetails.class);
    soft.assertThat(meta).isEqualTo(errorDetails);

    String serialized = MAPPER.writeValueAsString(meta);
    JsonNode serializedAsJsonNode = MAPPER.readValue(serialized, JsonNode.class);
    // Cannot "just" compare jsonNode + serializedAsJsonNode, because the integer values are added
    // as 'int's for the one and as 'long's for the other, which makes comparing those impossible.
    soft.assertThat(serializedAsJsonNode)
        .asInstanceOf(type(JsonNode.class))
        .extracting(
            n -> n.get("type"),
            n -> n.get("id"),
            n -> n.get("metadataLocation"),
            n -> n.get("sqlText"),
            n -> n.get("dialect"))
        .containsExactly(
            jsonNode.get("type"),
            jsonNode.get("id"),
            jsonNode.get("metadataLocation"),
            jsonNode.get("sqlText"),
            jsonNode.get("dialect"));

    NessieErrorDetails deserialized = MAPPER.readValue(serialized, GenericErrorDetails.class);
    soft.assertThat(deserialized)
        .extracting(NessieErrorDetails::getType)
        .isEqualTo(errorDetails.getType());
  }
}

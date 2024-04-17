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
package org.projectnessie.error;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.model.Conflict.ConflictType.UNEXPECTED_HASH;
import static org.projectnessie.model.Conflict.ConflictType.UNKNOWN;
import static org.projectnessie.model.Conflict.conflict;
import static org.projectnessie.model.JsonUtil.arrayNode;
import static org.projectnessie.model.JsonUtil.objectNode;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.ContentKey;

class TestNessieError {

  private static final ObjectMapper mapper = new ObjectMapper();
  public static final int HTTP_500_CODE = 500;
  public static final String HTTP_500_MESSAGE = "Internal Server Error";

  static Stream<Arguments> conflictDeserialization() {
    JsonNode ckJson = objectNode().set("elements", arrayNode().add("foo"));

    return Stream.of(
        arguments(
            objectNode()
                .put("conflictType", "UNEXPECTED_HASH")
                .put("message", "msg")
                .set("key", ckJson),
            conflict(UNEXPECTED_HASH, ContentKey.of("foo"), "msg")),
        arguments(
            objectNode().put("conflictType", "UNKNOWN").put("message", "msg").set("key", ckJson),
            conflict(UNKNOWN, ContentKey.of("foo"), "msg")),
        arguments(
            objectNode()
                .put("conflictType", "THIS_IS_SOME_NEW_CONFLICT_TYPE")
                .put("message", "msg")
                .set("key", ckJson),
            conflict(UNKNOWN, ContentKey.of("foo"), "msg")),
        arguments(
            objectNode()
                .put("conflictType", "THIS_IS_SOME_NEW_CONFLICT_TYPE")
                .put("message", "msg")
                .put("someNewField", "blah")
                .set("key", ckJson),
            conflict(UNKNOWN, ContentKey.of("foo"), "msg")),
        arguments(
            objectNode().putNull("conflictType").put("message", "msg"),
            conflict(UNKNOWN, null, "msg")));
  }

  /**
   * Verify that especially {@link Conflict} deserialization works properly, that unknown {@link
   * Conflict.ConflictType}s are properly mapped and that the lenient object-mapper does not fail
   * for unknown properties (special case for {@link org.projectnessie.error.NessieError}
   * deserialization).
   */
  @ParameterizedTest
  @MethodSource("conflictDeserialization")
  void conflictDeserialization(JsonNode input, Conflict expected) throws Exception {
    Conflict parsedConflict = new ObjectMapper().treeToValue(input, Conflict.class);
    Assertions.assertThat(parsedConflict).isEqualTo(expected);
  }

  @Test
  void fullMessage() {
    NessieError e =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.UNKNOWN)
            .status(HTTP_500_CODE)
            .reason(HTTP_500_MESSAGE)
            .serverStackTrace("foo.bar.InternalServerError\n" + "\tat some.other.Class")
            .build();
    assertThat(e.getFullMessage())
        .matches(
            HTTP_500_MESSAGE
                + " [(]HTTP/"
                + HTTP_500_CODE
                + "[)]: message\\R"
                + "foo.bar.InternalServerError\\R"
                + "\tat some.other.Class");

    e =
        ImmutableNessieError.builder()
            .from(e)
            .clientProcessingError(new Exception("processingException").toString())
            .build();
    assertThat(e.getFullMessage())
        .matches(
            // Using a regex here, because the stack trace looks different with
            // junit-platform-maven-plugin
            Pattern.compile(
                HTTP_500_MESSAGE
                    + " [(]HTTP/"
                    + HTTP_500_CODE
                    + "[)]: message\\R"
                    + "foo.bar.InternalServerError\\R"
                    + "\tat some.other.Class\\R"
                    + "Additionally, the client-side error below was caught while decoding the HTTP response: "
                    + "java.lang.Exception: processingException",
                Pattern.DOTALL));
  }

  @Test
  void jsonRoundTrip() throws JsonProcessingException {
    NessieError e0 =
        ImmutableNessieError.builder()
            .message("message")
            .errorCode(ErrorCode.UNKNOWN)
            .status(HTTP_500_CODE)
            .reason(HTTP_500_MESSAGE)
            .serverStackTrace("foo.bar.InternalServerError\n" + "\tat some.other.Class")
            .clientProcessingError(new Exception("processingException").toString())
            .build();

    String json = mapper.writeValueAsString(e0);
    NessieError e1 = mapper.readValue(json, NessieError.class);

    assertThat(e1.getClientProcessingError()).isNull(); // not propagated through JSON

    // Copy e0 without the client error
    NessieError e2 = ImmutableNessieError.builder().from(e0).clientProcessingError(null).build();
    assertThat(e1).isEqualTo(e2);
  }
}

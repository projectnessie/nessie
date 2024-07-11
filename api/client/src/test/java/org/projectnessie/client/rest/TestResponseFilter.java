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
package org.projectnessie.client.rest;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.error.ReferenceConflicts.referenceConflicts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieBackendThrottledException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieContentNotFoundException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.error.NessieUnavailableException;
import org.projectnessie.error.NessieUnsupportedMediaTypeException;
import org.projectnessie.error.ReferenceConflicts;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Conflict.ConflictType;
import org.projectnessie.model.ContentKey;
import software.amazon.awssdk.utils.StringInputStream;

@ExtendWith(SoftAssertionsExtension.class)
public class TestResponseFilter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("testReferenceConflicts")
  void testReferenceConflicts(JsonNode nessieError, ReferenceConflicts referenceConflicts) {
    soft.assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new TestResponseContext(Status.CONFLICT, nessieError)))
        .asInstanceOf(type(NessieReferenceConflictException.class))
        .extracting(NessieReferenceConflictException::getErrorDetails)
        .isEqualTo(referenceConflicts);
  }

  static Stream<Arguments> testReferenceConflicts() {
    Function<BiFunction<ObjectNode, ObjectNode, ObjectNode>, JsonNode> nessieErrorBuilder =
        p -> {
          ObjectNode referenceConflicts = objectNode().put("type", "REFERENCE_CONFLICTS");
          ObjectNode nessieError =
              objectNode()
                  .put("status", 409)
                  .put("reason", "something odd")
                  .put("message", "help me")
                  .put("errorCode", "REFERENCE_CONFLICT");
          referenceConflicts = p.apply(nessieError, referenceConflicts);
          if (referenceConflicts != null) {
            nessieError.set("errorDetails", referenceConflicts);
          }
          return nessieError;
        };

    return Stream.of(
        // 1 - null conflicts
        arguments(nessieErrorBuilder.apply((nessieError, refConflicts) -> null), null),
        // 2 - empty conflicts
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) -> refConflicts.set("conflicts", arrayNode())),
            referenceConflicts(emptyList())),
        // 3 - unknown property in ReferenceConflicts
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) ->
                    refConflicts
                        .put("someNewPropertyInReferenceConflicts", "bar")
                        .set("conflicts", arrayNode())),
            referenceConflicts(emptyList())),
        // 4 - single conflict
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) ->
                    refConflicts.set(
                        "conflicts",
                        arrayNode()
                            .add(
                                objectNode()
                                    .put("conflictType", "NAMESPACE_ABSENT")
                                    .put("message", "not there")
                                    .set(
                                        "key",
                                        objectNode().set("elements", arrayNode().add("ck")))))),
            referenceConflicts(
                singletonList(
                    Conflict.conflict(
                        ConflictType.NAMESPACE_ABSENT, ContentKey.of("ck"), "not there")))),
        // 5 - unknown conflict type
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) ->
                    refConflicts.set(
                        "conflicts",
                        arrayNode()
                            .add(
                                objectNode()
                                    .put("conflictType", "SOME_NEW_TYPE_WAS_ADDED")
                                    .put("message", "not there")
                                    .set(
                                        "key",
                                        objectNode().set("elements", arrayNode().add("ck")))))),
            referenceConflicts(
                singletonList(
                    Conflict.conflict(ConflictType.UNKNOWN, ContentKey.of("ck"), "not there")))),
        // 6 - new property in Conflict
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) ->
                    refConflicts.set(
                        "conflicts",
                        arrayNode()
                            .add(
                                objectNode()
                                    .put("conflictType", "KEY_EXISTS")
                                    .put("message", "not there")
                                    .put("someNewPropertyInConflict", 42)))),
            referenceConflicts(
                singletonList(Conflict.conflict(ConflictType.KEY_EXISTS, null, "not there")))),
        // 7 - new property in NessieError
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) -> {
                  nessieError.put("someNewPropertyInNessieError", 42);
                  return null;
                }),
            null),
        // 8 - simulate a new NessieErrorDetails type
        arguments(
            nessieErrorBuilder.apply(
                (nessieError, refConflicts) -> {
                  refConflicts.put("type", "SOME_NEW_ERROR_DETAILS_SUBTYPE").put("foo", "bar");
                  return refConflicts;
                }),
            null));
  }

  @ParameterizedTest
  @MethodSource("provider")
  void testResponseFilter(
      Status responseCode, ErrorCode errorCode, Class<? extends Exception> clazz) {
    NessieError error =
        ImmutableNessieError.builder()
            .message("test-error")
            .status(responseCode.getCode())
            .errorCode(errorCode)
            .reason(responseCode.getReason())
            .serverStackTrace("xxx")
            .build();
    try {
      ResponseCheckFilter.checkResponse(new TestResponseContext(responseCode, error));
    } catch (Exception e) {
      soft.assertThat(e).isInstanceOf(clazz);
      if (e instanceof NessieServiceException) {
        soft.assertThat(((NessieServiceException) e).getError()).isEqualTo(error);
      }
      if (e instanceof BaseNessieClientServerException) {
        soft.assertThat((BaseNessieClientServerException) e)
            .extracting(
                BaseNessieClientServerException::getStatus,
                BaseNessieClientServerException::getServerStackTrace)
            .containsExactly(error.getStatus(), error.getServerStackTrace());
      }
    }
  }

  @Test
  void testBadReturn() {
    NessieError error =
        ImmutableNessieError.builder().message("unknown").status(415).reason("xxx").build();
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new TestResponseContext(Status.UNSUPPORTED_MEDIA_TYPE, error)))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("xxx (HTTP/415): unknown");
  }

  @Test
  void testBadReturnNoError() {
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new ResponseContext() {
                      @Override
                      public Status getStatus() {
                        return Status.UNAUTHORIZED;
                      }

                      @Override
                      public InputStream getInputStream() {
                        return new StringInputStream("this will fail");
                      }

                      @Override
                      public boolean isJsonCompatibleResponse() {
                        return true;
                      }

                      @Override
                      public String getContentType() {
                        return null;
                      }

                      @Override
                      public URI getRequestedUri() {
                        return null;
                      }
                    }))
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("" + Status.UNAUTHORIZED.getCode())
        .hasMessageContaining(Status.UNAUTHORIZED.getReason())
        .hasMessageContaining("JsonParseException"); // from parsing `this will fail`
  }

  @Test
  void testUnexpectedError() {
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new ResponseContext() {
                      @Override
                      public Status getStatus() {
                        return Status.NOT_IMPLEMENTED;
                      }

                      @Override
                      public InputStream getInputStream() {
                        // Quarkus may sometimes produce JSON error responses like this
                        return new StringInputStream(
                            "{\"details\":\"Error id ee7f7293-67ad-42bd-8973-179801e7120e-1\",\"stack\":\"\"}");
                      }

                      @Override
                      public boolean isJsonCompatibleResponse() {
                        return true;
                      }

                      @Override
                      public String getContentType() {
                        return null;
                      }

                      @Override
                      public URI getRequestedUri() {
                        return null;
                      }
                    }))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("" + Status.NOT_IMPLEMENTED.getCode())
        .hasMessageContaining(Status.NOT_IMPLEMENTED.getReason())
        .hasMessageContaining("ee7f7293-67ad-42bd-8973-179801e7120e-1")
        .hasMessageContaining("Cannot build NessieError"); // jackson parse error
  }

  @Test
  void testBadReturnBadError() {
    assertThatThrownBy(
            () -> ResponseCheckFilter.checkResponse(new TestResponseContext(Status.UNAUTHORIZED)))
        .isInstanceOf(NessieNotAuthorizedException.class)
        .hasMessageContaining("" + Status.UNAUTHORIZED.getCode())
        .hasMessageContaining(Status.UNAUTHORIZED.getReason())
        .hasMessageContaining("Could not parse error object in response");
  }

  @Test
  void testGood() {
    assertDoesNotThrow(() -> ResponseCheckFilter.checkResponse(new TestResponseContext(Status.OK)));
  }

  private static Stream<Arguments> provider() {
    return Stream.of(
        arguments(Status.BAD_REQUEST, ErrorCode.UNKNOWN, RuntimeException.class),
        arguments(Status.BAD_REQUEST, ErrorCode.BAD_REQUEST, NessieBadRequestException.class),
        arguments(Status.UNAUTHORIZED, ErrorCode.UNKNOWN, NessieNotAuthorizedException.class),
        arguments(Status.FORBIDDEN, ErrorCode.FORBIDDEN, NessieForbiddenException.class),
        arguments(Status.FORBIDDEN, ErrorCode.UNKNOWN, NessieServiceException.class),
        arguments(Status.SERVICE_UNAVAILABLE, ErrorCode.UNKNOWN, NessieUnavailableException.class),
        arguments(
            Status.SERVICE_UNAVAILABLE,
            ErrorCode.SERVICE_UNAVAILABLE,
            NessieUnavailableException.class),
        arguments(Status.TOO_MANY_REQUESTS, ErrorCode.UNKNOWN, NessieServiceException.class),
        arguments(
            Status.TOO_MANY_REQUESTS,
            ErrorCode.TOO_MANY_REQUESTS,
            NessieBackendThrottledException.class),
        arguments(
            Status.NOT_FOUND, ErrorCode.CONTENT_NOT_FOUND, NessieContentNotFoundException.class),
        arguments(
            Status.NOT_FOUND,
            ErrorCode.REFERENCE_NOT_FOUND,
            NessieReferenceNotFoundException.class),
        arguments(Status.NOT_FOUND, ErrorCode.UNKNOWN, RuntimeException.class),
        arguments(Status.CONFLICT, ErrorCode.REFERENCE_CONFLICT, NessieConflictException.class),
        arguments(Status.CONFLICT, ErrorCode.UNKNOWN, RuntimeException.class),
        arguments(
            Status.UNSUPPORTED_MEDIA_TYPE,
            ErrorCode.UNSUPPORTED_MEDIA_TYPE,
            NessieUnsupportedMediaTypeException.class),
        arguments(
            Status.INTERNAL_SERVER_ERROR, ErrorCode.UNKNOWN, NessieInternalServerException.class));
  }

  private static class TestResponseContext implements ResponseContext {

    private final Status code;
    private final JsonNode error;

    TestResponseContext(Status code) {
      this.code = code;
      this.error = null;
    }

    TestResponseContext(Status code, NessieError error) {
      this.code = code;
      TokenBuffer tokenBuffer = new TokenBuffer(MAPPER, false);
      try {
        MAPPER.writeValue(tokenBuffer, error);
        this.error = tokenBuffer.asParser().readValueAsTree();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    TestResponseContext(Status code, JsonNode error) {
      this.code = code;
      this.error = error;
    }

    @Override
    public Status getStatus() {
      return code;
    }

    @Override
    public InputStream getInputStream() throws JsonProcessingException {
      if (error == null) {
        return null;
      }
      String value = MAPPER.writeValueAsString(error);
      return new StringInputStream(value);
    }

    @Override
    public boolean isJsonCompatibleResponse() {
      return true;
    }

    @Override
    public String getContentType() {
      return null;
    }

    @Override
    public URI getRequestedUri() {
      return null;
    }
  }

  static ObjectNode objectNode() {
    return JsonNodeFactory.instance.objectNode();
  }

  static ArrayNode arrayNode() {
    return JsonNodeFactory.instance.arrayNode();
  }
}

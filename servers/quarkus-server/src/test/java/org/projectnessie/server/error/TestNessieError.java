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
package org.projectnessie.server.error;

import static io.restassured.RestAssured.given;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.client.rest.NessieInternalServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieBackendThrottledException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieUnsupportedMediaTypeException;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;
import org.projectnessie.server.QuarkusNessieClientResolver;

/**
 * Test reported exceptions both for cases when {@code javax.validation} fails (when the Nessie
 * infra code isn't even run) and exceptions reported <em>by</em> Nessie.
 */
@QuarkusTest
@TestProfile(QuarkusTestProfilePersistInmemory.class)
@ExtendWith(QuarkusNessieClientResolver.class)
class TestNessieError {

  // Cannot use @ExtendWith(SoftAssertionsExtension.class) + @InjectSoftAssertions here, because
  // of Quarkus class loading issues. See https://github.com/quarkusio/quarkus/issues/19814
  protected final SoftAssertions soft = new SoftAssertions();

  @AfterEach
  public void afterEachAssert() {
    soft.assertAll();
  }

  private static HttpClient client;
  private static ObjectMapper mapper;
  private static URI errorTestUri;

  @BeforeAll
  static void setup(@NessieClientUri URI uri) {
    mapper =
        new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    errorTestUri = uri.resolve("../nessieErrorTest/");
    client =
        HttpClient.builder()
            .setBaseUri(errorTestUri)
            .setObjectMapper(mapper)
            .addResponseFilter(new NessieHttpResponseFilter())
            .build();
  }

  @Test
  void nullParameterQueryGet() {
    soft.assertThatThrownBy(() -> client.newRequest().path("nullParameterQueryGet").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): nullParameterQueryGet.hash: must not be null");
  }

  @Test
  void nullParameterQueryPost() {
    soft.assertThatThrownBy(() -> client.newRequest().path("nullParameterQueryPost").post(""))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): nullParameterQueryPost.hash: must not be null");
  }

  @Test
  void emptyParameterQueryGet() {
    soft.assertThatThrownBy(() -> client.newRequest().path("emptyParameterQueryGet").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): emptyParameterQueryGet.hash: must not be empty");
    soft.assertThatThrownBy(
            () -> client.newRequest().path("emptyParameterQueryGet").queryParam("hash", "").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): emptyParameterQueryGet.hash: must not be empty");
  }

  @Test
  void blankParameterQueryGet() {
    soft.assertThatThrownBy(() -> client.newRequest().path("blankParameterQueryGet").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): blankParameterQueryGet.hash: must not be blank");
    soft.assertThatThrownBy(
            () -> client.newRequest().path("blankParameterQueryGet").queryParam("hash", "").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): blankParameterQueryGet.hash: must not be blank");
    soft.assertThatThrownBy(
            () ->
                client.newRequest().path("blankParameterQueryGet").queryParam("hash", "   ").get())
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage("Bad Request (HTTP/400): blankParameterQueryGet.hash: must not be blank");
  }

  @Test
  void unsupportedMediaTypePut() {
    soft.assertThatThrownBy(
            () -> unwrap(() -> client.newRequest().path("unsupportedMediaTypePut").put("foo")))
        .isInstanceOf(NessieUnsupportedMediaTypeException.class)
        .hasMessage(
            "Unsupported Media Type (HTTP/415): The content-type header value did not match the value in @Consumes");
  }

  @Test
  void entityValueViolation() {
    soft.assertThatThrownBy(
            () -> client.newRequest().path("basicEntity").put("not really valid json"))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith(
            "Bad Request (HTTP/400): Unrecognized token 'not': was expecting (JSON String, Number, "
                + "Array, Object or token 'null', 'true' or 'false')\n");
    soft.assertThatThrownBy(() -> client.newRequest().path("basicEntity").put("{}"))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith("Bad Request (HTTP/400): ")
        .hasMessageContaining("value");
    soft.assertThatThrownBy(() -> client.newRequest().path("basicEntity").put("{\"value\":null}"))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith(
            "Bad Request (HTTP/400): basicEntity.entity.value: must not be null");
    soft.assertThatThrownBy(() -> client.newRequest().path("basicEntity").put("{\"value\":1.234}"))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessage(
            "Bad Request (HTTP/400): basicEntity.entity.value: must be greater than or equal to 3");
  }

  @Test
  void brokenEntitySerialization() {
    // send something that cannot be deserialized
    soft.assertThatThrownBy(
            () -> unwrap(() -> client.newRequest().path("basicEntity").put(new OtherEntity("bar"))))
        .isInstanceOf(NessieBadRequestException.class)
        .hasMessageStartingWith("Bad Request (HTTP/400): ")
        .hasMessageContaining("value");
  }

  @Test
  void nessieNotFoundException() {
    soft.assertThatThrownBy(() -> unwrap(() -> client.newRequest().path("nessieNotFound").get()))
        .isInstanceOf(NessieNotFoundException.class)
        .hasMessage("not-there-message")
        .asInstanceOf(InstanceOfAssertFactories.type(NessieNotFoundException.class))
        .matches(e -> e.getServerStackTrace() == null)
        .extracting(NessieNotFoundException::getStatus)
        .isEqualTo(Response.Status.NOT_FOUND.getStatusCode());
  }

  @Test
  void nonConstraintValidationExceptions() {
    // Exceptions that trigger the "else-ish" part in ResteasyExceptionMapper.toResponse()

    soft.assertThatThrownBy(
            () -> unwrap(() -> client.newRequest().path("constraintDefinitionException").get()))
        .isInstanceOf(NessieInternalServerException.class)
        .hasMessage(
            "Internal Server Error (HTTP/500): jakarta.validation.ConstraintDefinitionException: meep");
    soft.assertThatThrownBy(
            () -> unwrap(() -> client.newRequest().path("constraintDeclarationException").get()))
        .isInstanceOf(NessieInternalServerException.class)
        .hasMessage(
            "Internal Server Error (HTTP/500): jakarta.validation.ConstraintDeclarationException: meep");
    soft.assertThatThrownBy(
            () -> unwrap(() -> client.newRequest().path("groupDefinitionException").get()))
        .isInstanceOf(NessieInternalServerException.class)
        .hasMessage(
            "Internal Server Error (HTTP/500): jakarta.validation.GroupDefinitionException: meep");
  }

  @Test
  void unhandledRuntimeExceptionInStore() {
    // see org.projectnessie.server.error.ErrorTestService.unhandledExceptionInTvsStore
    soft.assertThatThrownBy(
            () -> client.newRequest().path("unhandledExceptionInTvsStore/runtime").get())
        .isInstanceOf(NessieInternalServerException.class)
        .hasMessage(
            "Internal Server Error (HTTP/500): java.lang.RuntimeException: Store.getValues-throwing");
  }

  @Test
  void backendThrottledExceptionInStore() {
    // see org.projectnessie.server.error.ErrorTestService.unhandledExceptionInTvsStore
    soft.assertThatThrownBy(
            () -> client.newRequest().path("unhandledExceptionInTvsStore/throttle").get())
        .isInstanceOf(NessieBackendThrottledException.class)
        .hasMessage(
            "Too Many Requests (HTTP/429): Backend store refused to process the request: "
                + "org.projectnessie.versioned.BackendLimitExceededException: Store.getValues-throttled");
  }

  @Test
  void malformedJsonErrorPayloadShape() throws Exception {
    JsonNode error =
        putBasicEntity("not really valid json", Response.Status.BAD_REQUEST.getStatusCode());

    assertNessieErrorPayload(
        error, Response.Status.BAD_REQUEST, ErrorCode.BAD_REQUEST, "Unrecognized token 'not'");
    assertNoDiagnosticFields(error);
  }

  @Test
  void validationErrorPayloadShape() throws Exception {
    JsonNode error = putBasicEntity("{}", Response.Status.BAD_REQUEST.getStatusCode());

    assertNessieErrorPayload(error, Response.Status.BAD_REQUEST, ErrorCode.BAD_REQUEST, "value");
    assertNoDiagnosticFields(error);
  }

  @Test
  void nessieNotFoundErrorPayloadShape() throws Exception {
    JsonNode error =
        readErrorPayload(
            given()
                .baseUri(errorTestUri.toString())
                .accept(ContentType.JSON)
                .get("nessieNotFound")
                .then()
                .statusCode(Response.Status.NOT_FOUND.getStatusCode())
                .contentType(ContentType.JSON)
                .extract()
                .asString());

    assertNessieErrorPayload(
        error, Response.Status.NOT_FOUND, ErrorCode.REFERENCE_NOT_FOUND, "not-there-message");
    assertNoDiagnosticFields(error);
  }

  @Test
  void throttledErrorPayloadShape() throws Exception {
    JsonNode error =
        readErrorPayload(
            given()
                .baseUri(errorTestUri.toString())
                .accept(ContentType.JSON)
                .get("unhandledExceptionInTvsStore/throttle")
                .then()
                .statusCode(Response.Status.TOO_MANY_REQUESTS.getStatusCode())
                .contentType(ContentType.JSON)
                .extract()
                .asString());

    assertNessieErrorPayload(
        error,
        Response.Status.TOO_MANY_REQUESTS,
        ErrorCode.TOO_MANY_REQUESTS,
        "Backend store refused to process the request");
    assertNoDiagnosticFields(error);
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof HttpClientException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }

  private static JsonNode putBasicEntity(String body, int expectedStatus) throws Exception {
    return readErrorPayload(
        given()
            .baseUri(errorTestUri.toString())
            .contentType(ContentType.JSON)
            .accept(ContentType.JSON)
            .body(body)
            .put("basicEntity")
            .then()
            .statusCode(expectedStatus)
            .contentType(ContentType.JSON)
            .extract()
            .asString());
  }

  private static JsonNode readErrorPayload(String json) throws Exception {
    return mapper.readTree(json);
  }

  private void assertNessieErrorPayload(
      JsonNode error, Response.Status status, ErrorCode errorCode, String messagePart) {
    soft.assertThat(error.get("status").asInt()).isEqualTo(status.getStatusCode());
    soft.assertThat(error.get("reason").asText()).isEqualTo(status.getReasonPhrase());
    soft.assertThat(error.get("errorCode").asText()).isEqualTo(errorCode.name());
    soft.assertThat(error.get("message").asText()).contains(messagePart);
  }

  private void assertNoDiagnosticFields(JsonNode error) {
    soft.assertThat(
            error.path("serverStackTrace").isNull()
                || error.path("serverStackTrace").isMissingNode())
        .isTrue();
    soft.assertThat(error.has("clientProcessingException")).isFalse();
    soft.assertThat(error.has("clientProcessingError")).isFalse();
  }
}

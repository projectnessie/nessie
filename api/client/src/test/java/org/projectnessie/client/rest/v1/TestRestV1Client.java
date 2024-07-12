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
package org.projectnessie.client.rest.v1;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import java.util.concurrent.atomic.AtomicReference;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.NessieApiCompatibilityException;
import org.projectnessie.client.rest.NessieBadResponseException;
import org.projectnessie.client.rest.NessieInternalServerException;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.client.rest.v2.HttpApiV2;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.client.util.HttpTestUtil;
import org.projectnessie.client.util.JaegerTestTracer;
import org.projectnessie.model.Branch;

@ExtendWith(SoftAssertionsExtension.class)
class TestRestV1Client {

  @InjectSoftAssertions SoftAssertions soft;

  static final String W3C_PROPAGATION_HEADER_NAME = "traceparent";

  @BeforeAll
  static void setupTracer() {
    JaegerTestTracer.register();
  }

  @Test
  void testNonJsonResponse() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("GET", req.getMethod());
          HttpTestUtil.writeResponseBody(resp, "<html>hello world>", "text/html");
        };
    try (HttpTestServer server = new HttpTestServer(handler);
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri())
                .withTracing(true)
                .withApiCompatibilityCheck(false)
                .build(NessieApiV1.class)) {
      assertThatThrownBy(api::getDefaultBranch)
          .isInstanceOf(NessieBadResponseException.class)
          .hasMessageStartingWith(
              "OK (HTTP/200): Expected the server to return a JSON compatible response, but the server replied with Content-Type 'text/html' from ");
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "application/json",
        "application/json;charset=utf-8",
        "text/json",
        "mystuff/foo+json",
        "mystuff/foo+json;charset=utf-8"
      })
  void testValidJsonResponse(String contentType) throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          Assertions.assertEquals("GET", req.getMethod());
          String response = new ObjectMapper().writeValueAsString(Branch.of("foo", "deadbeef"));
          HttpTestUtil.writeResponseBody(resp, response, contentType);
        };
    try (HttpTestServer server = new HttpTestServer("/trees/tree", handler);
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri())
                .withTracing(true)
                .withApiCompatibilityCheck(false)
                .build(NessieApiV1.class)) {
      api.getDefaultBranch();
    }
  }

  @Test
  void testTracing() throws Exception {
    AtomicReference<String> traceId = new AtomicReference<>();

    try (HttpTestServer server =
            new HttpTestServer(handlerForHeaderTest(W3C_PROPAGATION_HEADER_NAME, traceId));
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri())
                .withTracing(true)
                .build(NessieApiV1.class)) {
      OpenTelemetry otel = GlobalOpenTelemetry.get();
      Span span = otel.getTracer("nessie-client").spanBuilder("testOpenTracing").startSpan();
      try (Scope ignored = span.makeCurrent()) {
        api.getConfig();
      }
    }

    // Cannot really assert on the value of the Uber-trace-id header, because the APIs don't
    // give us access to that. Verifying that the Uber-trace-id header is being sent should
    // be good enough though.
    assertNotNull(traceId.get());
  }

  @Test
  void testTracingNotEnabled() throws Exception {
    AtomicReference<String> traceId = new AtomicReference<>();

    try (HttpTestServer server =
            new HttpTestServer(handlerForHeaderTest(W3C_PROPAGATION_HEADER_NAME, traceId));
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri())
                .withTracing(false)
                .build(NessieApiV1.class)) {
      OpenTelemetry otel = GlobalOpenTelemetry.get();
      Span span = otel.getTracer("nessie-client").spanBuilder("testOpenTracing").startSpan();
      try (Scope ignored = span.makeCurrent()) {
        api.getConfig();
      }
    }

    assertNull(traceId.get());
  }

  private HttpTestServer errorServer(int status) throws Exception {
    return new HttpTestServer(
        (req, resp) -> {
          resp.setStatus(status);
          resp.getOutputStream().close();
        });
  }

  @Test
  void testNotFoundOnBaseUri() throws Exception {
    try (HttpTestServer server = errorServer(404);
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri().resolve("/unknownPath"))
                .withApiCompatibilityCheck(false)
                .build(NessieApiV1.class)) {
      assertThatThrownBy(api::getConfig)
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Not Found");
    }
  }

  @Test
  void testInternalServerError() throws Exception {
    try (HttpTestServer server = errorServer(500);
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri().resolve("/broken"))
                .withApiCompatibilityCheck(false)
                .build(NessieApiV1.class)) {
      assertThatThrownBy(api::getConfig)
          .isInstanceOf(NessieInternalServerException.class)
          .hasMessageContaining("Internal Server Error");
    }
  }

  @Test
  void testUnauthorized() throws Exception {
    try (HttpTestServer server = errorServer(401);
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri().resolve("/unauthorized"))
                .withApiCompatibilityCheck(false)
                .build(NessieApiV1.class)) {
      assertThatThrownBy(api::getConfig)
          .isInstanceOf(NessieNotAuthorizedException.class)
          .hasMessageContaining("Unauthorized");
    }
  }

  @Test
  void testApiCompatibility() {
    // Good cases
    soft.assertThatCode(() -> testConfig(NessieApiV1.class, 1, 1, 1, true))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> testConfig(NessieApiV1.class, 1, 2, 1, true))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> testConfig(NessieApiV2.class, 1, 2, 2, true))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> testConfig(NessieApiV2.class, 2, 2, 2, true))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> testConfig(NessieApiV2.class, 2, 3, 2, true))
        .doesNotThrowAnyException();
    // Bad cases
    // 1. v1 client called a server that doesn't support v1
    soft.assertThatThrownBy(() -> testConfig(NessieApiV1.class, 2, 2, 2, true))
        .isInstanceOf(NessieApiCompatibilityException.class);
    // 2. v2 client called a server that doesn't support v2
    soft.assertThatThrownBy(() -> testConfig(NessieApiV2.class, 1, 1, 1, true))
        .isInstanceOf(NessieApiCompatibilityException.class);
    // 3. v1 client called a v2 endpoint
    soft.assertThatThrownBy(() -> testConfig(NessieApiV1.class, 1, 2, 2, true))
        .isInstanceOf(NessieApiCompatibilityException.class);
    // 4. v2 client called a v1 endpoint
    soft.assertThatThrownBy(() -> testConfig(NessieApiV2.class, 1, 2, 1, true))
        .isInstanceOf(NessieApiCompatibilityException.class);
    // Bad cases with compatibility check disabled
    // 1. v1 client called a server that doesn't support v1
    soft.assertThatCode(() -> testConfig(NessieApiV1.class, 2, 2, 2, false))
        .doesNotThrowAnyException();
    // 2. v2 client called a server that doesn't support v2
    soft.assertThatCode(() -> testConfig(NessieApiV2.class, 1, 1, 1, false))
        .doesNotThrowAnyException();
    // 3. v1 client called a v2 endpoint
    soft.assertThatCode(() -> testConfig(NessieApiV1.class, 1, 2, 2, false))
        .doesNotThrowAnyException();
    // 4. v2 client called a v1 endpoint
    soft.assertThatCode(() -> testConfig(NessieApiV2.class, 1, 2, 1, false))
        .doesNotThrowAnyException();
  }

  private void testConfig(
      Class<? extends NessieApiV1> apiClass, int min, int max, int actual, boolean check)
      throws Exception {
    try (HttpTestServer server = forConfig(min, max, actual);
        NessieApiV1 api =
            createClientBuilderFromSystemSettings()
                .withUri(server.getUri())
                .withApiCompatibilityCheck(check)
                .build(apiClass)) {
      api.getConfig();
    }
  }

  @Test
  void testCloseApiV1() {
    NessieApiClient client = mock(NessieApiClient.class);
    NessieApiV1 api = new HttpApiV1(client);
    api.close();
    verify(client).close();
  }

  @Test
  void testCloseApiV2() {
    HttpClient client = mock(HttpClient.class);
    NessieApiV2 api = new HttpApiV2(client);
    api.close();
    verify(client).close();
  }

  @Test
  void testCloseApiClient() {
    HttpClient client = mock(HttpClient.class);
    NessieApiClient apiClient = new RestV1Client(client);
    apiClient.close();
    verify(client).close();
  }

  static HttpTestServer.RequestHandler handlerForHeaderTest(
      String headerName, AtomicReference<String> receiver) {
    return (req, resp) -> {
      receiver.set(req.getHeader(headerName));
      req.getInputStream().close();
      resp.addHeader("Content-Type", "application/json");
      HttpTestUtil.writeResponseBody(resp, "{\"maxSupportedApiVersion\":1}");
    };
  }

  static HttpTestServer forConfig(int minApiVersion, int maxApiVersion, int actual)
      throws Exception {
    return new HttpTestServer(
        (req, resp) -> {
          req.getInputStream().close();
          resp.addHeader("Content-Type", "application/json");
          StringBuilder json =
              new StringBuilder()
                  .append("{\"minSupportedApiVersion\":")
                  .append(minApiVersion)
                  .append(",\"maxSupportedApiVersion\":")
                  .append(maxApiVersion);
          if (actual > 0) {
            json.append(",\"actualApiVersion\":").append(actual);
          }
          json.append("}");
          HttpTestUtil.writeResponseBody(resp, json.toString());
        });
  }
}

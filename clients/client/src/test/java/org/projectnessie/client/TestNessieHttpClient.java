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
package org.projectnessie.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.sun.net.httpserver.HttpHandler;
import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import java.io.OutputStream;
import java.net.URI;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.util.JaegerTestTracer;
import org.projectnessie.client.util.TestServer;

class TestNessieHttpClient {
  @BeforeAll
  static void setupTracer() {
    JaegerTestTracer.register();
  }

  @Test
  void testNullUri() {
    assertThatThrownBy(
            () -> HttpClientBuilder.builder().withUri((URI) null).build(NessieApiV1.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot construct Http client. Must have a non-null uri");
  }

  @Test
  void testAuthBasic() throws Exception {
    AtomicReference<String> authHeader = new AtomicReference<>();

    try (TestServer server = new TestServer(handlerForHeaderTest("Authorization", authHeader))) {
      NessieApiV1 api =
          HttpClientBuilder.builder()
              .withUri(server.getUri())
              .withAuthentication(BasicAuthenticationProvider.create("my_username", "very_secret"))
              .build(NessieApiV1.class);
      api.getConfig();
    }

    assertThat(authHeader.get())
        .isNotNull()
        .isEqualTo(
            "Basic "
                + new String(
                    Base64.getUrlEncoder().encode("my_username:very_secret".getBytes(UTF_8)),
                    UTF_8));
  }

  @Test
  void testTracing() throws Exception {
    AtomicReference<String> traceId = new AtomicReference<>();

    try (TestServer server = new TestServer(handlerForHeaderTest("Uber-trace-id", traceId))) {
      NessieApiV1 api =
          HttpClientBuilder.builder()
              .withUri(server.getUri())
              .withTracing(true)
              .build(NessieApiV1.class);
      try (Scope ignore =
          GlobalTracer.get()
              .activateSpan(GlobalTracer.get().buildSpan("testOpenTracing").start())) {
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

    try (TestServer server = new TestServer(handlerForHeaderTest("Uber-trace-id", traceId))) {
      NessieApiV1 api =
          HttpClientBuilder.builder()
              .withUri(server.getUri())
              .withTracing(false)
              .build(NessieApiV1.class);
      try (Scope ignore =
          GlobalTracer.get()
              .activateSpan(GlobalTracer.get().buildSpan("testOpenTracing").start())) {
        api.getConfig();
      }
    }

    assertNull(traceId.get());
  }

  static HttpHandler handlerForHeaderTest(String headerName, AtomicReference<String> receiver) {
    return h -> {
      receiver.set(h.getRequestHeaders().getFirst(headerName));

      h.getRequestBody().close();

      byte[] body = "{\"maxSupportedApiVersion\":1}".getBytes(UTF_8);
      h.sendResponseHeaders(200, body.length);
      try (OutputStream out = h.getResponseBody()) {
        out.write(body);
      }
    };
  }
}

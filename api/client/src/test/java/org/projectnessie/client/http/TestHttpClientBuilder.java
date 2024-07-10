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
package org.projectnessie.client.http;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.client.NessieClientBuilder.createClientBuilderFromSystemSettings;
import static org.projectnessie.client.NessieConfigConstants.CONF_ENABLE_API_COMPATIBILITY_CHECK;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApi;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.auth.BasicAuthenticationProvider;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.client.util.HttpTestUtil;

@SuppressWarnings("resource")
public class TestHttpClientBuilder {
  interface IncompatibleApiInterface extends NessieApi {}

  @Test
  void testIncompatibleApiInterface() {
    assertThatThrownBy(
            () ->
                createClientBuilderFromSystemSettings()
                    .withUri(URI.create("http://localhost"))
                    .build(IncompatibleApiInterface.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "API version org.projectnessie.client.http.TestHttpClientBuilder$IncompatibleApiInterface is not supported.");
  }

  @Test
  void testIncompatibleAuthProvider() {
    assertThatThrownBy(
            () ->
                createClientBuilderFromSystemSettings()
                    .withUri(URI.create("http://localhost"))
                    .withAuthentication(new NessieAuthentication() {})
                    .build(IncompatibleApiInterface.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("HttpClientBuilder only accepts instances of HttpAuthentication");
  }

  @Test
  void testInvalidUriScheme() {
    assertThatThrownBy(
            () ->
                createClientBuilderFromSystemSettings()
                    .withUri(URI.create("file:///foo/bar/baz"))
                    .build(NessieApiV1.class))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot start http client. file:///foo/bar/baz must be a valid http or https address");
  }

  @SuppressWarnings("deprecation")
  static List<Function<NessieClientBuilder, NessieClientBuilder>> basicAuthConfigs() {
    return Arrays.asList(
        cfg ->
            cfg.withAuthentication(
                BasicAuthenticationProvider.create("my_username", "very_secret")),
        cfg ->
            cfg.withAuthenticationFromConfig(
                prop -> {
                  switch (prop) {
                    case NessieConfigConstants.CONF_NESSIE_AUTH_TYPE:
                      return "BASIC";
                    case NessieConfigConstants.CONF_NESSIE_USERNAME:
                      return "my_username";
                    case NessieConfigConstants.CONF_NESSIE_PASSWORD:
                      return "very_secret";
                    default:
                      return null;
                  }
                }));
  }

  @ParameterizedTest
  @MethodSource("basicAuthConfigs")
  void testAuthBasic(Function<NessieClientBuilder, NessieClientBuilder> config) throws Exception {
    AtomicReference<String> authHeader = new AtomicReference<>();

    try (HttpTestServer server =
        new HttpTestServer(handlerForHeaderTest("Authorization", authHeader))) {
      try (NessieApiV1 client =
          config
              .apply(createClientBuilderFromSystemSettings().withUri(server.getUri()))
              .build(NessieApiV1.class)) {
        client.getConfig();
      }
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
  void testApiCompatibilityCheckDisabled() {
    assertThatCode(
            () ->
                createClientBuilderFromSystemSettings()
                    .withUri(URI.create("http://non-existent-host"))
                    .withApiCompatibilityCheck(false)
                    .build(NessieApiV2.class))
        .doesNotThrowAnyException();
  }

  @Test
  void testApiCompatibilityCheckDisabledWithSystemProperties() {
    System.setProperty(CONF_ENABLE_API_COMPATIBILITY_CHECK, "false");
    try {
      assertThatCode(
              () ->
                  createClientBuilderFromSystemSettings()
                      .withUri(URI.create("http://non-existent-host"))
                      .build(NessieApiV2.class))
          .doesNotThrowAnyException();
    } finally {
      System.clearProperty(CONF_ENABLE_API_COMPATIBILITY_CHECK);
    }
  }

  @Test
  void testRequestResponseFilter() throws Exception {
    try (HttpTestServer server =
        new HttpTestServer(
            (req, resp) -> {
              assertThat(req.getHeader("X-Test")).isEqualTo("test-value");
              req.getInputStream().close();
              HttpTestUtil.writeResponseBody(
                  resp, "{\"maxSupportedApiVersion\":1}", "application/test+json");
            })) {
      try (NessieApiV1 client =
          ((NessieHttpClientBuilder) createClientBuilderFromSystemSettings())
              .addRequestFilter((ctx) -> ctx.putHeader("X-Test", "test-value"))
              .addResponseFilter(
                  (ctx) -> assertThat(ctx.getContentType()).isEqualTo("application/test+json"))
              .withUri(server.getUri())
              .build(NessieApiV1.class)) {
        client.getConfig();
      }
    }
  }

  @Test
  void testBuilderFromBuilderWithNoSslCertificateVerification() {
    HttpClient.Builder builder1 =
        HttpClient.builder()
            .setSslNoCertificateVerification(true)
            .setObjectMapper(new ObjectMapper());
    assertThatCode(() -> builder1.build().close()).doesNotThrowAnyException();
  }

  @Test
  void testNoSslCertificateVerificationWithSslContext() throws NoSuchAlgorithmException {
    HttpClient.Builder builder1 =
        HttpClient.builder()
            .setSslNoCertificateVerification(true)
            .setSslContext(SSLContext.getDefault())
            .setObjectMapper(new ObjectMapper());
    assertThatIllegalArgumentException()
        .isThrownBy(() -> builder1.build().close())
        .withMessage(
            "Cannot construct Http client, must not combine nessie.ssl.no-certificate-verification and an explicitly configured SSLContext");
  }

  static HttpTestServer.RequestHandler handlerForHeaderTest(
      String headerName, AtomicReference<String> receiver) {
    return (req, resp) -> {
      receiver.set(req.getHeader(headerName));
      req.getInputStream().close();
      HttpTestUtil.writeResponseBody(resp, "{\"maxSupportedApiVersion\":1}");
    };
  }
}

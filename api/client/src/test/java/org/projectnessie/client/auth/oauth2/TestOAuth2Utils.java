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
package org.projectnessie.client.auth.oauth2;

import static java.time.Duration.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.client.util.HttpTestServer.RequestHandler;

class TestOAuth2Utils {

  @ParameterizedTest
  @ValueSource(ints = {1, 5, 10, 15, 20, 100})
  void testRandomAlphaNumString(int length) {
    String actual = OAuth2Utils.randomAlphaNumString(length);
    assertThat(actual).hasSize(length).matches("^[a-zA-Z0-9]*$");
  }

  private static final String DATA =
      "{"
          + "\"issuer\":\"http://server.com/realms/master\","
          + "\"authorization_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/auth\","
          + "\"token_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/token\""
          + "}";

  private static final String WRONG_DATA =
      "{"
          + "\"authorization_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/auth\","
          + "\"token_endpoint\":\"http://server.com/realms/master/protocol/openid-connect/token\""
          + "}";

  @ParameterizedTest
  @CsvSource({
    "''              , /.well-known/openid-configuration",
    "/               , /.well-known/openid-configuration",
    "''              , /.well-known/oauth-authorization-server",
    "/               , /.well-known/oauth-authorization-server",
    "/realms/master  , /realms/master/.well-known/openid-configuration",
    "/realms/master/ , /realms/master/.well-known/openid-configuration",
    "/realms/master  , /realms/master/.well-known/oauth-authorization-server",
    "/realms/master/ , /realms/master/.well-known/oauth-authorization-server"
  })
  void fetchOpenIdProviderMetadataSuccess(String issuerPath, String wellKnownPath)
      throws Exception {
    try (HttpTestServer server = new HttpTestServer(handler(wellKnownPath, DATA), true);
        HttpClient httpClient = newHttpClient(server)) {
      URI issuerUrl = server.getUri().resolve(issuerPath);
      JsonNode actual = OAuth2Utils.fetchOpenIdProviderMetadata(httpClient, issuerUrl);
      assertThat(actual).isEqualTo(OBJECT_MAPPER.readTree(DATA));
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongEndpoint() throws Exception {
    try (HttpTestServer server = new HttpTestServer(handler("/wrong/path", DATA), true);
        HttpClient httpClient = newHttpClient(server)) {
      URI issuerUrl = server.getUri().resolve("/realms/master/");
      Exception e =
          catchException(() -> OAuth2Utils.fetchOpenIdProviderMetadata(httpClient, issuerUrl));
      assertThat(e)
          .isInstanceOf(HttpClientException.class)
          .hasMessageContaining("Failed to fetch OpenID provider metadata");
      assertThat(e.getCause())
          .isInstanceOf(HttpClientException.class)
          .hasMessageContaining("404"); // messages differ between HttpClient impls
      assertThat(e.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(HttpClientException.class))
          .hasMessageContaining("404");
    }
  }

  @Test
  void fetchOpenIdProviderMetadataWrongData() throws Exception {
    try (HttpTestServer server =
            new HttpTestServer(
                handler("/realms/master/.well-known/openid-configuration", WRONG_DATA), true);
        HttpClient httpClient = newHttpClient(server)) {
      URI issuerUrl = server.getUri().resolve("/realms/master/");
      Exception e =
          catchException(() -> OAuth2Utils.fetchOpenIdProviderMetadata(httpClient, issuerUrl));
      assertThat(e)
          .isInstanceOf(HttpClientException.class)
          .hasMessageContaining("Failed to fetch OpenID provider metadata");
      assertThat(e.getCause())
          .isInstanceOf(HttpClientException.class)
          .hasMessage("Invalid OpenID provider metadata");
      assertThat(e.getSuppressed())
          .singleElement()
          .asInstanceOf(throwable(HttpClientException.class))
          .hasMessageContaining("404"); // messages differ between HttpClient impls
    }
  }

  @ParameterizedTest
  @MethodSource
  void testTokenExpirationTime(
      Instant now, Token token, Duration defaultLifespan, Instant expected) {
    Instant expirationTime = OAuth2Utils.tokenExpirationTime(now, token, defaultLifespan);
    assertThat(expirationTime).isEqualTo(expected);
  }

  static Stream<Arguments> testTokenExpirationTime() {
    Instant now = Instant.now();
    Duration defaultLifespan = Duration.ofHours(1);
    Instant customExpirationTime = now.plus(Duration.ofMinutes(1));
    return Stream.of(
        // expiration time from the token response => custom expiration time
        Arguments.of(
            now,
            ImmutableRefreshToken.builder()
                .payload("access-initial")
                .expirationTime(customExpirationTime)
                .build(),
            defaultLifespan,
            customExpirationTime),
        // no expiration time in the response, token is a JWT, exp claim present => exp claim
        Arguments.of(
            now,
            ImmutableRefreshToken.builder().payload(TestJwtToken.JWT_NON_EMPTY).build(),
            defaultLifespan,
            TestJwtToken.JWT_EXP_CLAIM),
        // no expiration time in the response, token is a JWT, but no exp claim => default lifespan
        Arguments.of(
            now,
            ImmutableRefreshToken.builder().payload(TestJwtToken.JWT_EMPTY).build(),
            defaultLifespan,
            now.plus(defaultLifespan)));
  }

  @ParameterizedTest
  @MethodSource
  void testShortestDelay(
      Instant now,
      Instant accessExp,
      Instant refreshExp,
      Duration safetyWindow,
      Duration minRefreshDelay,
      Duration expected) {
    Duration actual =
        OAuth2Utils.shortestDelay(now, accessExp, refreshExp, safetyWindow, minRefreshDelay);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testShortestDelay() {
    Instant now = Instant.now();
    Duration oneMinute = Duration.ofMinutes(1);
    Duration thirtySeconds = Duration.ofSeconds(30);
    Duration defaultWindow = Duration.ofSeconds(10);
    Duration oneSecond = Duration.ofSeconds(1);
    return Stream.of(
        // refresh token < access token
        Arguments.of(
            now,
            now.plus(oneMinute),
            now.plus(thirtySeconds),
            defaultWindow,
            oneSecond,
            thirtySeconds.minus(defaultWindow)),
        // refresh token > access token
        Arguments.of(
            now,
            now.plus(thirtySeconds),
            now.plus(oneMinute),
            defaultWindow,
            oneSecond,
            thirtySeconds.minus(defaultWindow)),
        // access token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now, now.minus(oneMinute), now.plus(oneMinute), defaultWindow, oneSecond, oneSecond),
        // refresh token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.minus(oneMinute), defaultWindow, oneSecond, oneSecond),
        // expirationTime - safety window > MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.plus(oneMinute), thirtySeconds, oneSecond, thirtySeconds),
        // expirationTime - safety window <= MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.plus(oneMinute), oneMinute, oneSecond, oneSecond),
        // expirationTime - safety window <= ZERO (immediate refresh use case)
        Arguments.of(now, now.plus(oneMinute), now.plus(oneMinute), oneMinute, ZERO, ZERO));
  }

  private RequestHandler handler(String wellKnownPath, String data) {
    return (req, resp) -> {
      if (req.getRequestURI().equals(wellKnownPath)) {
        resp.setStatus(200);
        resp.addHeader("Content-Type", "application/json;charset=UTF-8");
        resp.addHeader("Content-Length", String.valueOf(data.length()));
        resp.getOutputStream().write(data.getBytes(StandardCharsets.UTF_8));
      } else {
        resp.setStatus(404);
      }
    };
  }

  private static HttpClient newHttpClient(HttpTestServer server) {
    return HttpClient.builder()
        .setSslContext(server.getSslContext())
        .setObjectMapper(OBJECT_MAPPER)
        .build();
  }
}

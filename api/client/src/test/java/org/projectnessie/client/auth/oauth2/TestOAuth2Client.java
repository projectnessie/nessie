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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_REFRESH_DELAY;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.TestHttpClient;
import org.projectnessie.client.util.HttpTestServer;

@ExtendWith(SoftAssertionsExtension.class)
class TestOAuth2Client {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void testFetchNewTokens() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("POST");
          soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
          soft.assertThat(req.getHeader("Authorization")).isEqualTo("Basic QWxpY2U6czNjcjN0");
          Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
          NewTokensRequest request = MAPPER.convertValue(data, NewTokensRequest.class);
          soft.assertThat(request.getGrantType()).isEqualTo(NewTokensRequest.GRANT_TYPE);
          soft.assertThat(request.getScope()).isEqualTo("test");
          NewTokensResponse response =
              ImmutableNewTokensResponse.builder()
                  .tokenType("bearer")
                  .accessTokenPayload("cafebabe")
                  .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
                  .refreshTokenPayload("12345678")
                  .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(1)))
                  .scope("test")
                  .extraParameters(ImmutableMap.of("foo", "bar"))
                  .build();
          writeResponseBody(resp, response, "application/json");
        };

    try (HttpTestServer server = new HttpTestServer(handler, true)) {

      ImmutableOAuth2ClientParams params =
          ImmutableOAuth2ClientParams.builder()
              .tokenEndpoint(server.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
              .scope("test")
              .build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        NewTokensResponse tokens = ((NewTokensResponse) client.fetchNewTokens());
        assertThat(tokens.getAccessToken()).isNotNull();
        soft.assertThat(tokens.getAccessToken().getPayload()).isEqualTo("cafebabe");
        soft.assertThat(tokens.getAccessToken().getTokenType()).isEqualTo("bearer");
        soft.assertThat(tokens.getAccessToken().getExpirationTime())
            .isAfterOrEqualTo(NOW.plus(Duration.ofHours(1)).minusSeconds(10));
        assertThat(tokens.getRefreshToken()).isNotNull();
        soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("12345678");
        soft.assertThat(tokens.getRefreshToken().getExpirationTime())
            .isAfterOrEqualTo(NOW.plus(Duration.ofDays(1)).minusSeconds(10));
        soft.assertThat(tokens.getScope()).isEqualTo("test");
        soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
      }
    }
  }

  @Test
  void testRefreshTokens() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("POST");
          soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
          soft.assertThat(req.getHeader("Authorization")).isEqualTo("Basic QWxpY2U6czNjcjN0");
          Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
          RefreshTokensRequest request = MAPPER.convertValue(data, RefreshTokensRequest.class);
          soft.assertThat(request.getGrantType()).isEqualTo(RefreshTokensRequest.GRANT_TYPE);
          soft.assertThat(request.getRefreshToken()).isEqualTo("12345678");
          soft.assertThat(request.getScope()).isEqualTo("test");
          NewTokensResponse response =
              ImmutableNewTokensResponse.builder()
                  .tokenType("bearer")
                  .accessTokenPayload("cafebabe")
                  .accessTokenExpirationTime(NOW.plus(Duration.ofHours(2)))
                  .refreshTokenPayload("12345678")
                  .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(2)))
                  .scope("test")
                  .extraParameters(ImmutableMap.of("foo", "bar"))
                  .build();
          writeResponseBody(resp, response, "application/json");
        };
    try (HttpTestServer server = new HttpTestServer(handler, true)) {

      ImmutableOAuth2ClientParams params =
          ImmutableOAuth2ClientParams.builder()
              .tokenEndpoint(server.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
              .scope("test")
              .build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens =
            ImmutableNewTokensResponse.builder()
                .tokenType("bearer")
                .accessTokenPayload("cafebabe")
                .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
                .refreshTokenPayload("12345678")
                .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(1)))
                .scope("test")
                .build();
        RefreshTokensResponse tokens =
            ((RefreshTokensResponse) client.refreshTokens(currentTokens));
        assertThat(tokens.getAccessToken()).isNotNull();
        soft.assertThat(tokens.getAccessToken().getPayload()).isEqualTo("cafebabe");
        soft.assertThat(tokens.getAccessToken().getTokenType()).isEqualTo("bearer");
        soft.assertThat(tokens.getAccessToken().getExpirationTime())
            .isAfterOrEqualTo(NOW.plus(Duration.ofHours(2)).minusSeconds(10));
        assertThat(tokens.getRefreshToken()).isNotNull();
        soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("12345678");
        soft.assertThat(tokens.getRefreshToken().getExpirationTime())
            .isAfterOrEqualTo(NOW.plus(Duration.ofDays(2)).minusSeconds(10));
        soft.assertThat(tokens.getScope()).isEqualTo("test");
        soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
      }
    }
  }

  @Test
  void testExchangeTokens() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("POST");
          soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
          soft.assertThat(req.getHeader("Authorization")).isEqualTo("Basic QWxpY2U6czNjcjN0");
          Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
          TokensExchangeRequest request = MAPPER.convertValue(data, TokensExchangeRequest.class);
          soft.assertThat(request.getGrantType()).isEqualTo(TokensExchangeRequest.GRANT_TYPE);
          soft.assertThat(request.getSubjectToken()).isEqualTo("cafebabe");
          soft.assertThat(request.getSubjectTokenType())
              .isEqualTo(TokenTypeIdentifiers.ACCESS_TOKEN);
          soft.assertThat(request.getActorToken()).isNull();
          soft.assertThat(request.getActorTokenType()).isNull();
          soft.assertThat(request.getRequestedTokenType())
              .isEqualTo(TokenTypeIdentifiers.REFRESH_TOKEN);
          soft.assertThat(request.getScope()).isEqualTo("test");
          TokensExchangeResponse response =
              ImmutableTokensExchangeResponse.builder()
                  .issuedTokenType(TokenTypeIdentifiers.REFRESH_TOKEN)
                  .tokenType("bearer")
                  .accessTokenPayload("babecafe")
                  .accessTokenExpirationTime(NOW.plus(Duration.ofHours(2)))
                  .refreshTokenPayload("90abcdef")
                  .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(2)))
                  .scope("test")
                  .extraParameters(ImmutableMap.of("foo", "bar"))
                  .build();
          writeResponseBody(resp, response, "application/json");
        };
    try (HttpTestServer server = new HttpTestServer(handler, true)) {

      ImmutableOAuth2ClientParams params =
          ImmutableOAuth2ClientParams.builder()
              .tokenEndpoint(server.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
              .scope("test")
              .build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens =
            ImmutableNewTokensResponse.builder()
                .tokenType("bearer")
                .accessTokenPayload("cafebabe")
                .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
                .scope("test")
                .build();
        TokensExchangeResponse tokens =
            ((TokensExchangeResponse) client.exchangeTokens(currentTokens));
        assertThat(tokens.getAccessToken()).isNotNull();
        soft.assertThat(tokens.getAccessToken().getPayload()).isEqualTo("babecafe");
        soft.assertThat(tokens.getAccessToken().getTokenType()).isEqualTo("bearer");
        soft.assertThat(tokens.getAccessToken().getExpirationTime())
            .isAfterOrEqualTo(NOW.plus(Duration.ofHours(2)).minusSeconds(10));
        assertThat(tokens.getRefreshToken()).isNotNull();
        soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("90abcdef");
        soft.assertThat(tokens.getRefreshToken().getExpirationTime())
            .isAfterOrEqualTo(NOW.plus(Duration.ofDays(2)).minusSeconds(10));
        soft.assertThat(tokens.getIssuedTokenType()).isEqualTo(TokenTypeIdentifiers.REFRESH_TOKEN);
        soft.assertThat(tokens.getScope()).isEqualTo("test");
        soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
      }
    }
  }

  @Test
  void testRefreshTokenExpired() throws Exception {
    HttpTestServer.RequestHandler handler = (req, resp) -> {};
    try (HttpTestServer server = new HttpTestServer(handler, false)) {

      ImmutableOAuth2ClientParams params =
          ImmutableOAuth2ClientParams.builder()
              .tokenEndpoint(server.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
              .build();

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens =
            ImmutableNewTokensResponse.builder()
                .tokenType("bearer")
                .accessTokenPayload("cafebabe")
                .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
                .refreshTokenPayload("12345678")
                .refreshTokenExpirationTime(NOW.minus(Duration.ofSeconds(1)))
                .build();

        soft.assertThatThrownBy(() -> client.refreshTokens(currentTokens))
            .isInstanceOf(OAuth2Client.MustFetchNewTokensException.class)
            .hasMessage("Refresh token is about to expire");
      }
    }
  }

  @Test
  void testTokenExchangeDisabled() throws Exception {
    HttpTestServer.RequestHandler handler = (req, resp) -> {};
    try (HttpTestServer server = new HttpTestServer(handler, false)) {

      ImmutableOAuth2ClientParams params =
          ImmutableOAuth2ClientParams.builder()
              .tokenEndpoint(server.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
              .tokenExchangeEnabled(false)
              .scope("test")
              .build();

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens =
            ImmutableNewTokensResponse.builder()
                .tokenType("bearer")
                .accessTokenPayload("cafebabe")
                .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
                .scope("test")
                .build();

        soft.assertThatThrownBy(() -> client.exchangeTokens(currentTokens))
            .isInstanceOf(OAuth2Client.MustFetchNewTokensException.class)
            .hasMessage("Token exchange is disabled");
      }
    }
  }

  @Test
  void testBadRequest() throws Exception {
    HttpTestServer.RequestHandler handler =
        (req, resp) -> {
          soft.assertThat(req.getMethod()).isEqualTo("POST");
          soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
          soft.assertThat(req.getHeader("Authorization")).isEqualTo("Basic QWxpY2U6czNjcjN0");
          Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
          NewTokensRequest tokenRequest = MAPPER.convertValue(data, NewTokensRequest.class);
          soft.assertThat(tokenRequest.getGrantType()).isEqualTo(NewTokensRequest.GRANT_TYPE);
          soft.assertThat(tokenRequest.getScope()).isEqualTo("test");
          ErrorResponse response =
              ImmutableErrorResponse.builder()
                  .errorDescription("Try again")
                  .errorCode("invalid_request")
                  .build();
          writeResponseBody(resp, response, "application/json", Status.BAD_REQUEST.getCode());
        };

    try (HttpTestServer server = new HttpTestServer(handler, true)) {

      ImmutableOAuth2ClientParams params =
          ImmutableOAuth2ClientParams.builder()
              .tokenEndpoint(server.getUri())
              .clientId("Alice")
              .clientSecret("s3cr3t".getBytes(StandardCharsets.UTF_8))
              .scope("test")
              .build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        soft.assertThatThrownBy(client::fetchNewTokens)
            .isInstanceOf(OAuth2Exception.class)
            .extracting(OAuth2Exception.class::cast)
            .satisfies(
                r -> {
                  soft.assertThat(r.getMessage()).isEqualTo("Try again");
                  soft.assertThat(r.getErrorCode()).isEqualTo("invalid_request");
                });
      }
    }
  }

  @ParameterizedTest
  @MethodSource
  void testTokenExpirationTime(
      Instant now, Token token, Duration defaultLifespan, Instant expected) {
    Instant expirationTime = OAuth2Client.tokenExpirationTime(now, token, defaultLifespan);
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
                .payload("cafebabe")
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
  void testNextDelay(
      Instant now,
      Instant accessExp,
      Instant refreshExp,
      Duration safetyWindow,
      Duration expected) {
    Duration actual = OAuth2Client.nextDelay(now, accessExp, refreshExp, safetyWindow);
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testNextDelay() {
    Instant now = Instant.now();
    Duration oneMinute = Duration.ofMinutes(1);
    Duration thirtySeconds = Duration.ofSeconds(30);
    Duration defaultWindow = Duration.ofSeconds(10);
    return Stream.of(
        // refresh token < access token
        Arguments.of(
            now,
            now.plus(oneMinute),
            now.plus(thirtySeconds),
            defaultWindow,
            thirtySeconds.minus(defaultWindow)),
        // refresh token > access token
        Arguments.of(
            now,
            now.plus(thirtySeconds),
            now.plus(oneMinute),
            defaultWindow,
            thirtySeconds.minus(defaultWindow)),
        // access token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now, now.minus(oneMinute), now.plus(oneMinute), defaultWindow, MIN_REFRESH_DELAY),
        // refresh token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now, now.plus(oneMinute), now.minus(oneMinute), defaultWindow, MIN_REFRESH_DELAY),
        // expirationTime - safety window > MIN_REFRESH_DELAY
        Arguments.of(now, now.plus(oneMinute), now.plus(oneMinute), thirtySeconds, thirtySeconds),
        // expirationTime - safety window <= MIN_REFRESH_DELAY
        Arguments.of(now, now.plus(oneMinute), now.plus(oneMinute), oneMinute, MIN_REFRESH_DELAY));
  }
}

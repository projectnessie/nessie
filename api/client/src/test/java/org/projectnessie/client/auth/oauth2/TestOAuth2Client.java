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
import static org.assertj.core.api.AssertionsForClassTypes.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_REFRESH_DELAY;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.TestHttpClient;
import org.projectnessie.client.util.HttpTestServer;

@ExtendWith(SoftAssertionsExtension.class)
class TestOAuth2Client {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.now();

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void testBackgroundRefresh() throws Exception {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

    // handle the call to fetchNewTokens() for the initial token fetch
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));

    AtomicReference<Runnable> currentRenewalTask = new AtomicReference<>();

    // handle successive calls to scheduleTokensRenewal()
    when(executor.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              currentRenewalTask.set(runnable);
              return mock(ScheduledFuture.class);
            });

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      AtomicReference<Instant> i = new AtomicReference<>(NOW);
      ImmutableOAuth2ClientParams params =
          paramsBuilder(server).executor(executor).clock(i::get).build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {

        client.start();

        // should fetch the initial token
        AccessToken token = client.authenticate();
        assertThat(token.getPayload()).isEqualTo("access-initial");
        assertThat(client.getCurrentTokens()).isInstanceOf(ClientCredentialsTokensResponse.class);

        // emulate executor running the scheduled renewal task
        currentRenewalTask.get().run();

        // should have exchanged the token
        token = client.authenticate();
        assertThat(token.getPayload()).isEqualTo("access-exchanged");
        assertThat(client.getCurrentTokens()).isInstanceOf(TokensExchangeResponse.class);

        // emulate executor running the scheduled renewal task
        currentRenewalTask.get().run();

        // should have refreshed the token
        token = client.authenticate();
        assertThat(token.getPayload()).isEqualTo("access-refreshed");
        assertThat(client.getCurrentTokens()).isInstanceOf(RefreshTokensResponse.class);

        // emulate executor running the scheduled renewal task
        currentRenewalTask.get().run();

        // should have refreshed the token again
        token = client.authenticate();
        assertThat(token.getPayload()).isEqualTo("access-refreshed");
        assertThat(client.getCurrentTokens()).isInstanceOf(RefreshTokensResponse.class);

        // emulate executor running the scheduled renewal task and detecting that the client is idle
        // after 30+ seconds of inactivity
        i.set(i.get().plusSeconds(31));
        currentRenewalTask.get().run();
        assertThat(client.sleeping).isTrue();

        // should exit sleeping mode on next authenticate() call
        client.authenticate();
        assertThat(client.sleeping).isFalse();
      }
    }
  }

  @Test
  void testClientCredentials() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      ImmutableOAuth2ClientParams params = paramsBuilder(server).build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        ClientCredentialsTokensResponse tokens =
            ((ClientCredentialsTokensResponse) client.fetchNewTokens());
        checkInitialResponse(tokens, false);
      }
    }
  }

  @Test
  void testPassword() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      ImmutableOAuth2ClientParams params =
          paramsBuilder(server).grantType("password").username("Bob").password("s3cr3t").build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        PasswordTokensResponse tokens = ((PasswordTokensResponse) client.fetchNewTokens());
        checkInitialResponse(tokens, true);
      }
    }
  }

  @Test
  void testRefreshTokens() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      ImmutableOAuth2ClientParams params = paramsBuilder(server).build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens = getPasswordTokensResponse();
        RefreshTokensResponse tokens =
            ((RefreshTokensResponse) client.refreshTokens(currentTokens));
        checkRefreshResponse(tokens);
      }
    }
  }

  @Test
  void testExchangeTokens() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      ImmutableOAuth2ClientParams params = paramsBuilder(server).build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens = getClientCredentialsTokensResponse();
        TokensExchangeResponse tokens =
            ((TokensExchangeResponse) client.exchangeTokens(currentTokens));
        checkExchangeResponse(tokens);
      }
    }
  }

  @Test
  void testRefreshTokenExpired() throws Exception {
    HttpTestServer.RequestHandler handler = (req, resp) -> {};
    try (HttpTestServer server = new HttpTestServer(handler, false)) {

      ImmutableOAuth2ClientParams params = paramsBuilder(server).build();

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens =
            getPasswordTokensResponse()
                .withRefreshTokenExpirationTime(NOW.minus(Duration.ofSeconds(1)));

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
          paramsBuilder(server).tokenExchangeEnabled(false).build();

      try (OAuth2Client client = new OAuth2Client(params)) {
        Tokens currentTokens = getClientCredentialsTokensResponse();

        soft.assertThatThrownBy(() -> client.exchangeTokens(currentTokens))
            .isInstanceOf(OAuth2Client.MustFetchNewTokensException.class)
            .hasMessage("Token exchange is disabled");
      }
    }
  }

  @Test
  void testBadRequest() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      ImmutableOAuth2ClientParams params = paramsBuilder(server).scope("invalid-scope").build();
      params.getHttpClient().setSslContext(server.getSslContext());

      try (OAuth2Client client = new OAuth2Client(params)) {

        soft.assertThatThrownBy(client::fetchNewTokens)
            .isInstanceOf(OAuth2Exception.class)
            .extracting(OAuth2Exception.class::cast)
            .satisfies(
                r -> {
                  soft.assertThat(r.getErrorCode()).isEqualTo("invalid_request");
                  soft.assertThat(r.getMessage()).contains("Unknown scope: invalid-scope");
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
  void testNextDelay(
      Instant now,
      Instant accessExp,
      Instant refreshExp,
      Duration safetyWindow,
      Duration minRefreshDelay,
      Duration expected) {
    Duration actual =
        OAuth2Client.nextDelay(now, accessExp, refreshExp, safetyWindow, minRefreshDelay);
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
            MIN_REFRESH_DELAY,
            thirtySeconds.minus(defaultWindow)),
        // refresh token > access token
        Arguments.of(
            now,
            now.plus(thirtySeconds),
            now.plus(oneMinute),
            defaultWindow,
            MIN_REFRESH_DELAY,
            thirtySeconds.minus(defaultWindow)),
        // access token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now,
            now.minus(oneMinute),
            now.plus(oneMinute),
            defaultWindow,
            MIN_REFRESH_DELAY,
            MIN_REFRESH_DELAY),
        // refresh token already expired: MIN_REFRESH_DELAY
        Arguments.of(
            now,
            now.plus(oneMinute),
            now.minus(oneMinute),
            defaultWindow,
            MIN_REFRESH_DELAY,
            MIN_REFRESH_DELAY),
        // expirationTime - safety window > MIN_REFRESH_DELAY
        Arguments.of(
            now,
            now.plus(oneMinute),
            now.plus(oneMinute),
            thirtySeconds,
            MIN_REFRESH_DELAY,
            thirtySeconds),
        // expirationTime - safety window <= MIN_REFRESH_DELAY
        Arguments.of(
            now,
            now.plus(oneMinute),
            now.plus(oneMinute),
            oneMinute,
            MIN_REFRESH_DELAY,
            MIN_REFRESH_DELAY),
        // expirationTime - safety window <= ZERO (immediate refresh use case)
        Arguments.of(now, now.plus(oneMinute), now.plus(oneMinute), oneMinute, ZERO, ZERO));
  }

  private HttpTestServer.RequestHandler handler() {
    return (req, resp) -> {
      soft.assertThat(req.getMethod()).isEqualTo("POST");
      soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
      soft.assertThat(req.getHeader("Authorization")).isEqualTo("Basic QWxpY2U6czNjcjN0");
      Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
      if (data.containsKey("scope") && data.get("scope").equals("invalid-scope")) {
        ErrorResponse response =
            ImmutableErrorResponse.builder()
                .errorCode("invalid_request")
                .errorDescription("Unknown scope: invalid-scope")
                .build();
        writeResponseBody(resp, response, "application/json", 400);
        return;
      }
      TokensRequestBase request;
      TokensResponseBase response;
      int statusCode = 200;
      switch (data.get("grant_type")) {
        case ClientCredentialsTokensRequest.GRANT_TYPE:
          request = MAPPER.convertValue(data, ClientCredentialsTokensRequest.class);
          soft.assertThat(request.getScope()).isEqualTo("test");
          response = getClientCredentialsTokensResponse();
          break;
        case PasswordTokensRequest.GRANT_TYPE:
          request = MAPPER.convertValue(data, PasswordTokensRequest.class);
          soft.assertThat(request.getScope()).isEqualTo("test");
          soft.assertThat(((PasswordTokensRequest) request).getUsername()).isEqualTo("Bob");
          soft.assertThat(((PasswordTokensRequest) request).getPassword()).isEqualTo("s3cr3t");
          response = getPasswordTokensResponse();
          break;
        case RefreshTokensRequest.GRANT_TYPE:
          request = MAPPER.convertValue(data, RefreshTokensRequest.class);
          soft.assertThat(request.getScope()).isEqualTo("test");
          soft.assertThat(((RefreshTokensRequest) request).getRefreshToken())
              .isIn("refresh-initial", "refresh-refreshed", "refresh-exchanged");
          response = getRefreshTokensResponse();
          break;
        case TokensExchangeRequest.GRANT_TYPE:
          request = MAPPER.convertValue(data, TokensExchangeRequest.class);
          soft.assertThat(request.getScope()).isEqualTo("test");
          soft.assertThat(((TokensExchangeRequest) request).getSubjectToken())
              .isEqualTo("access-initial");
          soft.assertThat(((TokensExchangeRequest) request).getSubjectTokenType())
              .isEqualTo(TokenTypeIdentifiers.ACCESS_TOKEN);
          soft.assertThat(((TokensExchangeRequest) request).getActorToken()).isNull();
          soft.assertThat(((TokensExchangeRequest) request).getActorTokenType()).isNull();
          soft.assertThat(((TokensExchangeRequest) request).getRequestedTokenType())
              .isEqualTo(TokenTypeIdentifiers.REFRESH_TOKEN);
          response = getTokensExchangeResponse();
          break;
        default:
          throw new AssertionError("Unexpected grant type: " + data.get("grant_type"));
      }
      writeResponseBody(resp, response, "application/json", statusCode);
    };
  }

  private static ImmutableClientCredentialsTokensResponse getClientCredentialsTokensResponse() {
    return ImmutableClientCredentialsTokensResponse.builder()
        .tokenType("bearer")
        .accessTokenPayload("access-initial")
        .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
        // no refresh token
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private static ImmutablePasswordTokensResponse getPasswordTokensResponse() {
    return ImmutablePasswordTokensResponse.builder()
        .tokenType("bearer")
        .accessTokenPayload("access-initial")
        .accessTokenExpirationTime(NOW.plus(Duration.ofHours(1)))
        .refreshTokenPayload("refresh-initial")
        .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(1)))
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private static ImmutableRefreshTokensResponse getRefreshTokensResponse() {
    return ImmutableRefreshTokensResponse.builder()
        .tokenType("bearer")
        .accessTokenPayload("access-refreshed")
        .accessTokenExpirationTime(NOW.plus(Duration.ofHours(2)))
        .refreshTokenPayload("refresh-refreshed")
        .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(2)))
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private static ImmutableTokensExchangeResponse getTokensExchangeResponse() {
    return ImmutableTokensExchangeResponse.builder()
        .issuedTokenType(TokenTypeIdentifiers.REFRESH_TOKEN)
        .tokenType("bearer")
        .accessTokenPayload("access-exchanged")
        .accessTokenExpirationTime(NOW.plus(Duration.ofHours(3)))
        .refreshTokenPayload("refresh-exchanged")
        .refreshTokenExpirationTime(NOW.plus(Duration.ofDays(3)))
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private void checkInitialResponse(TokensResponseBase response, boolean expectRefreshToken) {
    soft.assertThat(response.getAccessToken()).isNotNull();
    soft.assertThat(response.getAccessToken().getPayload()).isEqualTo("access-initial");
    soft.assertThat(response.getAccessToken().getTokenType()).isEqualTo("bearer");
    soft.assertThat(response.getAccessToken().getExpirationTime())
        .isAfterOrEqualTo(NOW.plus(Duration.ofHours(1)).minusSeconds(10));
    if (expectRefreshToken) {
      assertThat(response.getRefreshToken()).isNotNull();
      soft.assertThat(response.getRefreshToken().getPayload()).isEqualTo("refresh-initial");
      soft.assertThat(response.getRefreshToken().getExpirationTime())
          .isAfterOrEqualTo(NOW.plus(Duration.ofDays(1)).minusSeconds(10));
    } else {
      assertThat(response.getRefreshToken()).isNull();
    }
    soft.assertThat(response.getScope()).isEqualTo("test");
    soft.assertThat(response.getExtraParameters()).containsExactly(entry("foo", "bar"));
  }

  private void checkRefreshResponse(TokensResponseBase tokens) {
    assertThat(tokens.getAccessToken()).isNotNull();
    soft.assertThat(tokens.getAccessToken().getPayload()).isEqualTo("access-refreshed");
    soft.assertThat(tokens.getAccessToken().getTokenType()).isEqualTo("bearer");
    soft.assertThat(tokens.getAccessToken().getExpirationTime())
        .isAfterOrEqualTo(NOW.plus(Duration.ofHours(2)).minusSeconds(10));
    assertThat(tokens.getRefreshToken()).isNotNull();
    soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("refresh-refreshed");
    soft.assertThat(tokens.getRefreshToken().getExpirationTime())
        .isAfterOrEqualTo(NOW.plus(Duration.ofDays(2)).minusSeconds(10));
    soft.assertThat(tokens.getScope()).isEqualTo("test");
    soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
  }

  private void checkExchangeResponse(TokensExchangeResponse tokens) {
    assertThat(tokens.getAccessToken()).isNotNull();
    soft.assertThat(tokens.getAccessToken().getPayload()).isEqualTo("access-exchanged");
    soft.assertThat(tokens.getAccessToken().getTokenType()).isEqualTo("bearer");
    soft.assertThat(tokens.getAccessToken().getExpirationTime())
        .isAfterOrEqualTo(NOW.plus(Duration.ofHours(3)).minusSeconds(10));
    assertThat(tokens.getRefreshToken()).isNotNull();
    soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("refresh-exchanged");
    soft.assertThat(tokens.getRefreshToken().getExpirationTime())
        .isAfterOrEqualTo(NOW.plus(Duration.ofDays(3)).minusSeconds(10));
    soft.assertThat(tokens.getScope()).isEqualTo("test");
    soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
    soft.assertThat(tokens.getIssuedTokenType()).isEqualTo(TokenTypeIdentifiers.REFRESH_TOKEN);
  }

  private static ImmutableOAuth2ClientParams.Builder paramsBuilder(HttpTestServer server) {
    return ImmutableOAuth2ClientParams.builder()
        .tokenEndpoint(server.getUri())
        .clientId("Alice")
        .clientSecret("s3cr3t")
        .scope("test");
  }
}

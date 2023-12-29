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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientConfig.OBJECT_MAPPER;
import static org.projectnessie.client.util.HttpTestUtil.writeResponseBody;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.TestHttpClient;
import org.projectnessie.client.http.impl.HttpUtils;
import org.projectnessie.client.util.HttpTestServer;
import org.projectnessie.client.util.HttpTestServer.RequestHandler;

@ExtendWith(SoftAssertionsExtension.class)
class TestOAuth2Client {

  private static final Instant START = Instant.parse("2023-01-01T00:00:00Z");

  @InjectSoftAssertions protected SoftAssertions soft;

  private Instant now;

  @BeforeEach
  void resetClock() {
    now = START;
  }

  @Test
  void testBackgroundRefresh() throws Exception {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

    mockInitialTokenFetch(executor);
    AtomicReference<Runnable> currentRenewalTask = mockTokensRefreshSchedule(executor);

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      OAuth2ClientConfig config = configBuilder(server, false).executor(executor).build();

      try (OAuth2Client client = new OAuth2Client(config)) {

        client.start();

        // should fetch the initial token
        AccessToken token = client.authenticate();
        soft.assertThat(token.getPayload()).isEqualTo("access-initial");
        soft.assertThat(client.getCurrentTokens())
            .isInstanceOf(ClientCredentialsTokensResponse.class);

        // emulate executor running the scheduled renewal task
        currentRenewalTask.get().run();

        // should have exchanged the token
        token = client.authenticate();
        soft.assertThat(token.getPayload()).isEqualTo("access-exchanged");
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(TokensExchangeResponse.class);

        // emulate executor running the scheduled renewal task
        currentRenewalTask.get().run();

        // should have refreshed the token
        token = client.authenticate();
        soft.assertThat(token.getPayload()).isEqualTo("access-refreshed");
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(RefreshTokensResponse.class);

        // emulate executor running the scheduled renewal task
        currentRenewalTask.get().run();

        // should have refreshed the token again
        token = client.authenticate();
        soft.assertThat(token.getPayload()).isEqualTo("access-refreshed");
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(RefreshTokensResponse.class);

        // emulate executor running the scheduled renewal task and detecting that the client is idle
        // after 30+ seconds of inactivity
        now = now.plusSeconds(31);
        currentRenewalTask.get().run();
        soft.assertThat(client.sleeping).isTrue();

        // should exit sleeping mode on next authenticate() call
        // and schedule a token refresh
        client.authenticate();
        soft.assertThat(client.sleeping).isFalse();

        // emulate executor running the scheduled renewal task and detecting that the client is idle
        // again after 30+ seconds of inactivity
        now = now.plusSeconds(31);
        currentRenewalTask.get().run();
        soft.assertThat(client.sleeping).isTrue();

        // should exit sleeping mode on next authenticate() call
        // and refresh tokens immediately because the current ones are expired
        // (in this test the tokens expire in two hours)
        now = now.plus(Duration.ofHours(2));
        client.authenticate();
        soft.assertThat(client.sleeping).isFalse();
      }
    }
  }

  @Test
  void testExecutionRejectedInitialTokenFetch() throws Exception {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    doThrow(RejectedExecutionException.class).when(executor).execute(any(Runnable.class));
    AtomicReference<Runnable> currentRenewalTask = mockTokensRefreshSchedule(executor);

    // If the executor rejects the initial token fetch, a call to authenticate()
    // throws RejectedExecutionException immediately.

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {
      OAuth2ClientConfig config = configBuilder(server, false).executor(executor).build();

      try (OAuth2Client client = new OAuth2Client(config)) {

        client.start();
        soft.assertThatThrownBy(client::authenticate)
            .hasCauseInstanceOf(RejectedExecutionException.class);

        // should have scheduled a refresh, when that refresh is executed successfully,
        // client should recover
        soft.assertThat(currentRenewalTask.get()).isNotNull();
        currentRenewalTask.get().run();
        client.authenticate();
      }
    }
  }

  @Test
  void testExecutionRejectedSubsequentTokenRefreshes() throws Exception {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    mockInitialTokenFetch(executor);
    AtomicReference<Runnable> currentRenewalTask = mockTokensRefreshSchedule(executor, true);

    // If the executor rejects a scheduled token refresh,
    // sleep mode should be activated; the first call to authenticate()
    // will trigger wake up, then refresh the token immediately (synchronously) if necessary,
    // then schedule a new refresh. If that refresh is rejected again,
    // sleep mode is reactivated.

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      OAuth2ClientConfig config = configBuilder(server, false).executor(executor).build();

      try (OAuth2Client client = new OAuth2Client(config)) {

        // will trigger token fetch (successful), then schedule a refresh, then reject it,
        // then sleep
        client.start();
        soft.assertThat(client.sleeping).isTrue();
        soft.assertThat(client.getCurrentTokens())
            .isInstanceOf(ClientCredentialsTokensResponse.class);
        soft.assertThat(currentRenewalTask.get()).isNull();

        // will wake up, then reject scheduling the next refresh, then sleep again,
        // then return the previously fetched token since it's still valid.
        AccessToken token = client.authenticate();
        soft.assertThat(client.sleeping).isTrue();
        soft.assertThat(token.getPayload()).isEqualTo("access-initial");
        soft.assertThat(client.getCurrentTokens())
            .isInstanceOf(ClientCredentialsTokensResponse.class);
        soft.assertThat(currentRenewalTask.get()).isNull();

        // will wake up, then refresh the token immediately (since it's expired),
        // then reject scheduling the next refresh, then sleep again,
        // then return the newly-fetched token
        now = now.plus(Duration.ofHours(1));
        token = client.authenticate();
        soft.assertThat(client.sleeping).isTrue();
        soft.assertThat(token.getPayload()).isEqualTo("access-exchanged");
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(TokensExchangeResponse.class);
        soft.assertThat(currentRenewalTask.get()).isNull();
      }
    }
  }

  @Test
  void testFailureRecovery() throws Exception {

    ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
    mockInitialTokenFetch(executor);
    AtomicReference<Runnable> currentRenewalTask = mockTokensRefreshSchedule(executor);

    RequestHandler defaultHandler = handler();
    RequestHandler failureHandler =
        (req, resp) -> {
          ErrorResponse response =
              ImmutableErrorResponse.builder()
                  .errorCode("transient")
                  .errorDescription("Transient error")
                  .build();
          writeResponseBody(resp, response, "application/json", 400);
        };
    AtomicReference<RequestHandler> handlerRef = new AtomicReference<>();
    RequestHandler handler = (req, resp) -> handlerRef.get().handle(req, resp);

    try (HttpTestServer server = new HttpTestServer(handler, true)) {

      OAuth2ClientConfig config = configBuilder(server, false).executor(executor).build();

      try (OAuth2Client client = new OAuth2Client(config)) {

        // simple failure recovery scenarios

        // Emulate failure on initial token fetch
        // => propagate the error but schedule a refresh ASAP
        handlerRef.set(failureHandler);
        client.start();
        Runnable renewalTask = currentRenewalTask.get();
        soft.assertThat(renewalTask).isNotNull();
        soft.assertThatThrownBy(client::authenticate).isInstanceOf(OAuth2Exception.class);

        // Emulate executor running the scheduled refresh task, then throwing an exception
        // => propagate the error but schedule another refresh
        handlerRef.set(failureHandler);
        renewalTask.run();
        soft.assertThat(currentRenewalTask.get()).isNotNull().isNotSameAs(renewalTask);
        renewalTask = currentRenewalTask.get();
        soft.assertThatThrownBy(client::authenticate).isInstanceOf(OAuth2Exception.class);

        // Emulate executor running the scheduled refresh task again, then finally getting tokens
        // => should recover and return initial tokens + schedule next refresh
        handlerRef.set(defaultHandler);
        renewalTask.run();
        soft.assertThat(currentRenewalTask.get()).isNotNull().isNotSameAs(renewalTask);
        renewalTask = currentRenewalTask.get();
        AccessToken token = client.authenticate();
        soft.assertThat(token.getPayload()).isEqualTo("access-initial");
        soft.assertThat(client.getCurrentTokens())
            .isInstanceOf(ClientCredentialsTokensResponse.class);

        // failure recovery when in sleep mode

        // Emulate executor running the scheduled refresh task again, getting tokens,
        // then setting sleeping to true because idle interval is past
        now = now.plusSeconds(31);
        renewalTask.run();
        soft.assertThat(currentRenewalTask.get()).isSameAs(renewalTask);
        soft.assertThat(client.sleeping).isTrue();
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(TokensExchangeResponse.class);

        // Emulate waking up when current token has expired,
        // then getting an error when renewing tokens immediately
        // => should propagate the error but schedule another refresh
        handlerRef.set(failureHandler);
        now = now.plus(Duration.ofHours(3));
        soft.assertThatThrownBy(client::authenticate).isInstanceOf(OAuth2Exception.class);
        soft.assertThat(client.sleeping).isFalse();
        soft.assertThat(currentRenewalTask.get()).isNotNull().isNotSameAs(renewalTask);
        renewalTask = currentRenewalTask.get();

        // Emulate executor running the scheduled refresh task again,
        // then getting an error, then setting sleeping to true again because idle interval is past
        now = now.plusSeconds(31);
        renewalTask.run();
        soft.assertThat(currentRenewalTask.get()).isSameAs(renewalTask);
        soft.assertThat(client.sleeping).isTrue();
        soft.assertThatThrownBy(client::getCurrentTokens).isInstanceOf(OAuth2Exception.class);

        // Emulate waking up, then fetching tokens immediately because no tokens are available,
        // then scheduling next refresh
        handlerRef.set(defaultHandler);
        token = client.authenticate();
        soft.assertThat(client.sleeping).isFalse();
        soft.assertThat(token.getPayload()).isEqualTo("access-initial");
        soft.assertThat(client.getCurrentTokens())
            .isInstanceOf(ClientCredentialsTokensResponse.class);
        soft.assertThat(currentRenewalTask.get()).isNotSameAs(renewalTask);
        renewalTask = currentRenewalTask.get();

        // Emulate executor running the scheduled refresh task again, exchanging tokens,
        // then setting sleeping to true again because idle interval is past
        now = now.plusSeconds(31);
        renewalTask.run();
        soft.assertThat(currentRenewalTask.get()).isSameAs(renewalTask);
        soft.assertThat(client.sleeping).isTrue();
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(TokensExchangeResponse.class);

        // Emulate waking up, then rescheduling a refresh since current token is still valid
        handlerRef.set(defaultHandler);
        token = client.authenticate();
        soft.assertThat(client.sleeping).isFalse();
        soft.assertThat(token.getPayload()).isEqualTo("access-exchanged");
        soft.assertThat(client.getCurrentTokens()).isInstanceOf(TokensExchangeResponse.class);
        soft.assertThat(currentRenewalTask.get()).isNotSameAs(renewalTask);
      }
    }
  }

  @Test
  void testClientCredentials() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      OAuth2ClientConfig config = configBuilder(server, false).build();

      try (OAuth2Client client = new OAuth2Client(config)) {
        ClientCredentialsTokensResponse tokens =
            ((ClientCredentialsTokensResponse) client.fetchNewTokens());
        checkInitialResponse(tokens, false);
      }
    }
  }

  @Test
  void testEndpointDiscovery() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      OAuth2ClientConfig config = configBuilder(server, true).build();

      try (OAuth2Client client = new OAuth2Client(config)) {
        ClientCredentialsTokensResponse tokens =
            ((ClientCredentialsTokensResponse) client.fetchNewTokens());
        checkInitialResponse(tokens, false);
      }
    }
  }

  @Test
  void testPassword() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      OAuth2ClientConfig config =
          configBuilder(server, false)
              .grantType(GrantType.PASSWORD)
              .username("Bob")
              .password("s3cr3t")
              .build();

      try (OAuth2Client client = new OAuth2Client(config)) {
        PasswordTokensResponse tokens = ((PasswordTokensResponse) client.fetchNewTokens());
        checkInitialResponse(tokens, true);
      }
    }
  }

  @Test
  void testAuthorizationCode() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), false)) {

      OAuth2ClientConfig config =
          configBuilder(server, false).grantType(GrantType.AUTHORIZATION_CODE).build();

      try (ResourceOwnerEmulator resourceOwner =
              new ResourceOwnerEmulator(GrantType.AUTHORIZATION_CODE);
          HttpClient tokenEndpointClient =
              config
                  .newHttpClientBuilder()
                  .setAuthentication(config.getBasicAuthentication())
                  .setBaseUri(server.getUri().resolve("/token"))
                  .build();
          AuthorizationCodeFlow flow =
              new AuthorizationCodeFlow(
                  config, tokenEndpointClient, resourceOwner.getConsoleOut())) {
        resourceOwner.setErrorListener(e -> flow.close());
        Tokens tokens = flow.fetchNewTokens();
        checkInitialResponse((TokensResponseBase) tokens, false);
      }
    }
  }

  @Test
  void testAuthorizationCodeTimeout() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), false)) {

      OAuth2ClientConfig config =
          configBuilder(server, false)
              .grantType(GrantType.AUTHORIZATION_CODE)
              .minAuthorizationCodeFlowTimeout(Duration.ofMillis(100))
              .authorizationCodeFlowTimeout(Duration.ofMillis(100))
              .build();

      try (HttpClient tokenEndpointClient =
              config
                  .newHttpClientBuilder()
                  .setAuthentication(config.getBasicAuthentication())
                  .setBaseUri(server.getUri().resolve("/token"))
                  .build();
          AuthorizationCodeFlow flow = new AuthorizationCodeFlow(config, tokenEndpointClient)) {

        soft.assertThatThrownBy(flow::fetchNewTokens)
            .hasMessageContaining("Timed out waiting waiting for authorization code")
            .hasCauseInstanceOf(TimeoutException.class);
      }
    }
  }

  @Test
  void testDeviceCode() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), false)) {

      OAuth2ClientConfig config =
          configBuilder(server, false)
              .grantType(GrantType.DEVICE_CODE)
              .minDeviceCodeFlowPollInterval(Duration.ofMillis(100))
              .deviceCodeFlowPollInterval(Duration.ofMillis(100))
              .build();

      try (ResourceOwnerEmulator resourceOwner = new ResourceOwnerEmulator(GrantType.DEVICE_CODE);
          HttpClient tokenEndpointClient =
              config
                  .newHttpClientBuilder()
                  .setAuthentication(config.getBasicAuthentication())
                  .setBaseUri(server.getUri().resolve("/token"))
                  .build();
          DeviceCodeFlow flow =
              new DeviceCodeFlow(config, tokenEndpointClient, resourceOwner.getConsoleOut())) {
        resourceOwner.setErrorListener(e -> flow.close());
        Tokens tokens = flow.fetchNewTokens();
        checkInitialResponse((TokensResponseBase) tokens, false);
      }
    }
  }

  @Test
  void testDeviceCodeTimeout() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), false)) {

      OAuth2ClientConfig config =
          configBuilder(server, false)
              .grantType(GrantType.DEVICE_CODE)
              .minDeviceCodeFlowTimeout(Duration.ofMillis(100))
              .deviceCodeFlowTimeout(Duration.ofMillis(100))
              .build();

      try (HttpClient tokenEndpointClient =
              config
                  .newHttpClientBuilder()
                  .setAuthentication(config.getBasicAuthentication())
                  .setBaseUri(server.getUri().resolve("/token"))
                  .build();
          DeviceCodeFlow flow = new DeviceCodeFlow(config, tokenEndpointClient)) {

        soft.assertThatThrownBy(flow::fetchNewTokens)
            .hasMessageContaining("Timed out waiting for user to authorize device")
            .hasCauseInstanceOf(TimeoutException.class);
      }
    }
  }

  @Test
  void testRefreshTokens() throws Exception {

    try (HttpTestServer server = new HttpTestServer(handler(), true)) {

      OAuth2ClientConfig config = configBuilder(server, false).build();

      try (OAuth2Client client = new OAuth2Client(config)) {
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

      OAuth2ClientConfig config = configBuilder(server, false).build();

      try (OAuth2Client client = new OAuth2Client(config)) {
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

      OAuth2ClientConfig config = configBuilder(server, false).build();

      try (OAuth2Client client = new OAuth2Client(config)) {
        Tokens currentTokens =
            getPasswordTokensResponse()
                .withRefreshTokenExpirationTime(now.minus(Duration.ofSeconds(1)));

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

      OAuth2ClientConfig config = configBuilder(server, false).tokenExchangeEnabled(false).build();

      try (OAuth2Client client = new OAuth2Client(config)) {
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

      OAuth2ClientConfig config = configBuilder(server, false).scope("invalid-scope").build();

      try (OAuth2Client client = new OAuth2Client(config)) {

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
  void testShortestDelay(
      Instant now,
      Instant accessExp,
      Instant refreshExp,
      Duration safetyWindow,
      Duration minRefreshDelay,
      Duration expected) {
    Duration actual =
        OAuth2Client.shortestDelay(now, accessExp, refreshExp, safetyWindow, minRefreshDelay);
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

  private class TestRequestHandler implements HttpTestServer.RequestHandler {

    private volatile boolean deviceAuthorized = false;

    @Override
    public void handle(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      String requestUri = req.getRequestURI();
      switch (requestUri) {
        case "/token":
          handleTokenEndpoint(req, resp);
          break;
        case "/auth":
          handleAuthorizationEndpoint(req, resp);
          break;
        case "/device-auth":
          handleDeviceAuthEndpoint(req, resp);
          break;
        case "/device":
          handleDeviceEndpoint(req, resp);
          break;
        case "/.well-known/openid-configuration":
          handleOpenIdProviderMetadataEndpoint(req, resp);
          break;
        default:
          throw new AssertionError("Unexpected request URI: " + requestUri);
      }
    }

    private void handleTokenEndpoint(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
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
      Object response;
      int statusCode = 200;
      String grantType = data.get("grant_type");
      if (grantType.equals(GrantType.CLIENT_CREDENTIALS.canonicalName())) {
        request = OBJECT_MAPPER.convertValue(data, ClientCredentialsTokensRequest.class);
        soft.assertThat(request.getScope()).isEqualTo("test");
        response = getClientCredentialsTokensResponse();
      } else if (grantType.equals(GrantType.PASSWORD.canonicalName())) {
        request = OBJECT_MAPPER.convertValue(data, PasswordTokensRequest.class);
        soft.assertThat(request.getScope()).isEqualTo("test");
        soft.assertThat(((PasswordTokensRequest) request).getUsername()).isEqualTo("Bob");
        soft.assertThat(((PasswordTokensRequest) request).getPassword()).isEqualTo("s3cr3t");
        response = getPasswordTokensResponse();
      } else if (grantType.equals(GrantType.REFRESH_TOKEN.canonicalName())) {
        request = OBJECT_MAPPER.convertValue(data, RefreshTokensRequest.class);
        soft.assertThat(request.getScope()).isEqualTo("test");
        soft.assertThat(((RefreshTokensRequest) request).getRefreshToken())
            .isIn("refresh-initial", "refresh-refreshed", "refresh-exchanged");
        response = getRefreshTokensResponse();
      } else if (grantType.equals(GrantType.TOKEN_EXCHANGE.canonicalName())) {
        request = OBJECT_MAPPER.convertValue(data, TokensExchangeRequest.class);
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
      } else if (grantType.equals(GrantType.AUTHORIZATION_CODE.canonicalName())) {
        request = OBJECT_MAPPER.convertValue(data, AuthorizationCodeTokensRequest.class);
        soft.assertThat(request.getScope()).isEqualTo("test");
        soft.assertThat(((AuthorizationCodeTokensRequest) request).getCode())
            .isEqualTo("test-code");
        soft.assertThat(((AuthorizationCodeTokensRequest) request).getRedirectUri())
            .contains("http://localhost:")
            .contains("/nessie-client/auth");
        soft.assertThat(((AuthorizationCodeTokensRequest) request).getClientId())
            .isEqualTo("Alice");
        response = getAuthorizationCodeTokensResponse();
      } else if (grantType.equals(GrantType.DEVICE_CODE.canonicalName())) {
        if (deviceAuthorized) {
          request = OBJECT_MAPPER.convertValue(data, DeviceCodeTokensRequest.class);
          soft.assertThat(request.getScope()).isEqualTo("test");
          soft.assertThat(((DeviceCodeTokensRequest) request).getDeviceCode())
              .isEqualTo("device-code");
          response = getDeviceAuthorizationTokensResponse();
        } else {
          response =
              ImmutableErrorResponse.builder()
                  .errorCode("authorization_pending")
                  .errorDescription("Authorization pending")
                  .build();
          statusCode = 401;
        }
      } else if (grantType.equals("mock_transient_error")) {
        response =
            ImmutableErrorResponse.builder()
                .errorCode("invalid_request")
                .errorDescription("Something went wrong (not really)")
                .build();
        statusCode = 400;
      } else {
        throw new AssertionError("Unexpected grant type: " + data.get("grant_type"));
      }
      writeResponseBody(resp, response, "application/json", statusCode);
    }

    private void handleAuthorizationEndpoint(HttpServletRequest req, HttpServletResponse resp) {
      soft.assertThat(req.getMethod()).isEqualTo("GET");
      Map<String, String> data = HttpUtils.parseQueryString(req.getQueryString());
      soft.assertThat(data)
          .containsEntry("response_type", "code")
          .containsEntry("client_id", "Alice")
          .containsEntry("scope", "test")
          .containsKey("redirect_uri")
          .containsKey("state");
      String redirectUri = data.get("redirect_uri") + "?code=test-code&state=" + data.get("state");
      resp.addHeader("Location", redirectUri);
      resp.setStatus(302);
    }

    private void handleDeviceAuthEndpoint(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
      soft.assertThat(req.getMethod()).isEqualTo("POST");
      soft.assertThat(req.getContentType()).isEqualTo("application/x-www-form-urlencoded");
      soft.assertThat(req.getHeader("Authorization")).isEqualTo("Basic QWxpY2U6czNjcjN0");
      Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
      soft.assertThat(data).containsEntry("scope", "test");
      URI uri = URI.create(req.getRequestURL().toString());
      DeviceCodeResponse response =
          ImmutableDeviceCodeResponse.builder()
              .deviceCode("device-code")
              .userCode("CAFE-BABE")
              .verificationUri(uri.resolve("/device"))
              .verificationUriComplete(uri.resolve("/device"))
              .expiresIn(Duration.ofMinutes(5))
              .interval(Duration.ofMillis(1))
              .build();
      writeResponseBody(resp, response, "application/json");
    }

    private void handleDeviceEndpoint(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {
      soft.assertThat(req.getMethod()).isIn("GET", "POST");
      if (req.getMethod().equals("GET")) {
        writeResponseBody(resp, "<html><body>Enter device code:</body></html>");
      } else {
        Map<String, String> data = TestHttpClient.decodeFormData(req.getInputStream());
        soft.assertThat(data).containsEntry("device_user_code", "CAFE-BABE");
        deviceAuthorized = true;
        writeResponseBody(resp, "{\"success\":true}");
      }
    }

    private void handleOpenIdProviderMetadataEndpoint(
        HttpServletRequest req, HttpServletResponse resp) throws IOException {
      URI uri = URI.create(req.getRequestURL().toString());
      ObjectNode node = JsonNodeFactory.instance.objectNode();
      node.put("issuer", uri.resolve("/").toString())
          .put("token_endpoint", uri.resolve("/token").toString())
          .put("authorization_endpoint", uri.resolve("/auth").toString())
          .put("device_authorization_endpoint", uri.resolve("/device-auth").toString());
      writeResponseBody(resp, node, "application/json", 200);
    }
  }

  private HttpTestServer.RequestHandler handler() {
    return new TestRequestHandler();
  }

  private ImmutableClientCredentialsTokensResponse getClientCredentialsTokensResponse() {
    return ImmutableClientCredentialsTokensResponse.builder()
        .tokenType("bearer")
        .accessTokenPayload("access-initial")
        .accessTokenExpirationTime(now.plus(Duration.ofHours(1)))
        // no refresh token
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private ImmutableAuthorizationCodeTokensResponse getAuthorizationCodeTokensResponse() {
    return ImmutableAuthorizationCodeTokensResponse.builder()
        .from(getClientCredentialsTokensResponse())
        .build();
  }

  private ImmutableDeviceCodeTokensResponse getDeviceAuthorizationTokensResponse() {
    return ImmutableDeviceCodeTokensResponse.builder()
        .from(getClientCredentialsTokensResponse())
        .build();
  }

  private ImmutablePasswordTokensResponse getPasswordTokensResponse() {
    return ImmutablePasswordTokensResponse.builder()
        .tokenType("bearer")
        .accessTokenPayload("access-initial")
        .accessTokenExpirationTime(now.plus(Duration.ofHours(1)))
        .refreshTokenPayload("refresh-initial")
        .refreshTokenExpirationTime(now.plus(Duration.ofDays(1)))
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private ImmutableRefreshTokensResponse getRefreshTokensResponse() {
    return ImmutableRefreshTokensResponse.builder()
        .tokenType("bearer")
        .accessTokenPayload("access-refreshed")
        .accessTokenExpirationTime(now.plus(Duration.ofHours(2)))
        .refreshTokenPayload("refresh-refreshed")
        .refreshTokenExpirationTime(now.plus(Duration.ofDays(2)))
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private ImmutableTokensExchangeResponse getTokensExchangeResponse() {
    return ImmutableTokensExchangeResponse.builder()
        .issuedTokenType(TokenTypeIdentifiers.REFRESH_TOKEN)
        .tokenType("bearer")
        .accessTokenPayload("access-exchanged")
        .accessTokenExpirationTime(now.plus(Duration.ofHours(3)))
        .refreshTokenPayload("refresh-exchanged")
        .refreshTokenExpirationTime(now.plus(Duration.ofDays(3)))
        .scope("test")
        .extraParameters(ImmutableMap.of("foo", "bar"))
        .build();
  }

  private void checkInitialResponse(TokensResponseBase response, boolean expectRefreshToken) {
    soft.assertThat(response.getAccessToken()).isNotNull();
    soft.assertThat(response.getAccessToken().getPayload()).isEqualTo("access-initial");
    soft.assertThat(response.getAccessToken().getTokenType()).isEqualTo("bearer");
    soft.assertThat(response.getAccessToken().getExpirationTime())
        .isAfterOrEqualTo(now.plus(Duration.ofHours(1)).minusSeconds(10));
    if (expectRefreshToken) {
      assertThat(response.getRefreshToken()).isNotNull();
      soft.assertThat(response.getRefreshToken().getPayload()).isEqualTo("refresh-initial");
      soft.assertThat(response.getRefreshToken().getExpirationTime())
          .isAfterOrEqualTo(now.plus(Duration.ofDays(1)).minusSeconds(10));
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
        .isAfterOrEqualTo(now.plus(Duration.ofHours(2)).minusSeconds(10));
    assertThat(tokens.getRefreshToken()).isNotNull();
    soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("refresh-refreshed");
    soft.assertThat(tokens.getRefreshToken().getExpirationTime())
        .isAfterOrEqualTo(now.plus(Duration.ofDays(2)).minusSeconds(10));
    soft.assertThat(tokens.getScope()).isEqualTo("test");
    soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
  }

  private void checkExchangeResponse(TokensExchangeResponse tokens) {
    assertThat(tokens.getAccessToken()).isNotNull();
    soft.assertThat(tokens.getAccessToken().getPayload()).isEqualTo("access-exchanged");
    soft.assertThat(tokens.getAccessToken().getTokenType()).isEqualTo("bearer");
    soft.assertThat(tokens.getAccessToken().getExpirationTime())
        .isAfterOrEqualTo(now.plus(Duration.ofHours(3)).minusSeconds(10));
    assertThat(tokens.getRefreshToken()).isNotNull();
    soft.assertThat(tokens.getRefreshToken().getPayload()).isEqualTo("refresh-exchanged");
    soft.assertThat(tokens.getRefreshToken().getExpirationTime())
        .isAfterOrEqualTo(now.plus(Duration.ofDays(3)).minusSeconds(10));
    soft.assertThat(tokens.getScope()).isEqualTo("test");
    soft.assertThat(tokens.getExtraParameters()).containsExactly(entry("foo", "bar"));
    soft.assertThat(tokens.getIssuedTokenType()).isEqualTo(TokenTypeIdentifiers.REFRESH_TOKEN);
  }

  private OAuth2ClientConfig.Builder configBuilder(HttpTestServer server, boolean discovery) {
    OAuth2ClientConfig.Builder builder =
        OAuth2ClientConfig.builder()
            .clientId("Alice")
            .clientSecret("s3cr3t")
            .scope("test")
            .clock(() -> now);
    if (server.getSslContext() != null) {
      builder.sslContext(server.getSslContext());
    }
    if (discovery) {
      builder.issuerUrl(server.getUri().resolve("/"));
    } else {
      builder
          .tokenEndpoint(server.getUri().resolve("/token"))
          .authEndpoint(server.getUri().resolve("/auth"))
          .deviceAuthEndpoint(server.getUri().resolve("/device-auth"));
    }
    return builder;
  }

  /** handle the call to fetchNewTokens() for the initial token fetch. */
  private static void mockInitialTokenFetch(ScheduledExecutorService executor) {
    doAnswer(
            invocation -> {
              Runnable runnable = invocation.getArgument(0);
              runnable.run();
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
  }

  /** Handle successive calls to scheduleTokensRenewal(). */
  private static AtomicReference<Runnable> mockTokensRefreshSchedule(
      ScheduledExecutorService executor) {
    return mockTokensRefreshSchedule(executor, false);
  }

  /** Handle successive calls to scheduleTokensRenewal() with option to reject scheduled tasks. */
  private static AtomicReference<Runnable> mockTokensRefreshSchedule(
      ScheduledExecutorService executor, boolean rejectSchedule) {
    AtomicReference<Runnable> task = new AtomicReference<>();
    when(executor.schedule(any(Runnable.class), anyLong(), any()))
        .thenAnswer(
            invocation -> {
              if (rejectSchedule) {
                throw new RejectedExecutionException("test");
              }
              Runnable runnable = invocation.getArgument(0);
              task.set(runnable);
              return mock(ScheduledFuture.class);
            });
    return task;
  }
}

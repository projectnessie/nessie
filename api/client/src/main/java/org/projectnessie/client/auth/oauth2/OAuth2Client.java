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

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS;
import static org.projectnessie.client.auth.oauth2.OAuth2ClientParams.MIN_REFRESH_DELAY;
import static org.projectnessie.client.auth.oauth2.TokenTypeIdentifiers.ACCESS_TOKEN;
import static org.projectnessie.client.auth.oauth2.TokenTypeIdentifiers.REFRESH_TOKEN;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple OAuth2 client that supports the "client_credentials" and "password" grant types for
 * fetching new access tokens, using pre-defined client credentials.
 *
 * <p>This client also supports refreshing access tokens using both the refresh token method defined
 * in RFC 6749 and the token exchange method defined in RFC 8693.
 *
 * <p>If you don't need to refresh access tokens, you can use the {@link
 * org.projectnessie.client.auth.BearerAuthenticationProvider BearerAuthenticationProvider} instead
 * and provide the token to use directly through configuration.
 */
class OAuth2Client implements OAuth2Authenticator, Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Client.class);
  private static final Duration MIN_WARN_INTERVAL = Duration.ofSeconds(10);

  private final String grantType;
  private final String username;
  private final byte[] password;
  private final String scope;
  private final Duration defaultAccessTokenLifespan;
  private final Duration defaultRefreshTokenLifespan;
  private final Duration refreshSafetyWindow;
  private final Duration idleInterval;
  private final boolean tokenExchangeEnabled;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;
  private final boolean shouldCloseExecutor;
  private final ObjectMapper objectMapper;
  private final CompletableFuture<Void> started = new CompletableFuture<>();
  /* Visible for testing. */ final AtomicBoolean sleeping = new AtomicBoolean();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final Supplier<Instant> clock;

  private volatile CompletionStage<Tokens> currentTokensStage;
  private volatile ScheduledFuture<?> tokenRefreshFuture;
  private volatile Instant lastAccess;
  private volatile Instant lastWarn;

  OAuth2Client(OAuth2ClientParams params) {
    grantType = params.getGrantType();
    username = params.getUsername().orElse(null);
    password = params.getPassword().map(s -> s.getBytes(StandardCharsets.UTF_8)).orElse(null);
    scope = params.getScope().orElse(null);
    defaultAccessTokenLifespan = params.getDefaultAccessTokenLifespan();
    defaultRefreshTokenLifespan = params.getDefaultRefreshTokenLifespan();
    refreshSafetyWindow = params.getRefreshSafetyWindow();
    idleInterval = params.getPreemptiveTokenRefreshIdleTimeout();
    tokenExchangeEnabled = params.getTokenExchangeEnabled();
    httpClient = params.getHttpClient().addResponseFilter(this::checkErrorResponse).build();
    executor = params.getExecutor();
    shouldCloseExecutor = executor instanceof OAuth2TokenRefreshExecutor;
    objectMapper = params.getObjectMapper();
    clock = params.getClock();
    lastAccess = clock.get();
    currentTokensStage = started.thenApplyAsync((v) -> fetchNewTokens(), executor);
    currentTokensStage
        .whenComplete((tokens, error) -> log(error))
        .whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
  }

  @Override
  public AccessToken authenticate() {
    Instant now = clock.get();
    lastAccess = now;
    if (sleeping.compareAndSet(true, false)) {
      wakeUp(now);
    }
    return getCurrentTokens().getAccessToken();
  }

  /** Visible for testing. */
  Tokens getCurrentTokens() {
    try {
      return currentTokensStage.toCompletableFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) {
        throw (Error) cause;
      } else {
        throw new RuntimeException("Cannot acquire a valid OAuth2 access token", cause);
      }
    }
  }

  private Tokens getCurrentTokensIfAvailable() {
    try {
      return currentTokensStage.toCompletableFuture().getNow(null);
    } catch (CancellationException | CompletionException ignored) {
    }
    return null;
  }

  /**
   * Starts the client.
   *
   * <p>Calling this method will trigger a first access token fetch. It will also schedule a refresh
   * of the access token when needed.
   *
   * <p>Calling this method twice or more is a no-op.
   */
  public void start() {
    started.complete(null);
  }

  @Override
  public void close() {
    if (closing.compareAndSet(false, true)) {
      LOGGER.debug("Closing...");
      try {
        currentTokensStage.toCompletableFuture().cancel(true);
        ScheduledFuture<?> tokenRefreshFuture = this.tokenRefreshFuture;
        if (tokenRefreshFuture != null) {
          tokenRefreshFuture.cancel(true);
        }
        if (shouldCloseExecutor) {
          if (!executor.isShutdown()) {
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
              executor.shutdownNow();
            }
          }
        }
        if (password != null) {
          Arrays.fill(password, (byte) 0);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        tokenRefreshFuture = null;
      }
      LOGGER.debug("Closed");
    }
  }

  private void wakeUp(Instant now) {
    LOGGER.debug("Waking up...");
    Tokens currentTokens = getCurrentTokensIfAvailable();
    Duration delay = nextTokenRefresh(currentTokens, now, Duration.ZERO);
    if (delay.compareTo(MIN_REFRESH_DELAY) < 0) {
      LOGGER.debug("Refreshing tokens immediately");
      renewTokens();
    } else {
      LOGGER.debug("Tokens are still valid");
      scheduleTokensRenewal(delay);
    }
  }

  private void maybeScheduleTokensRenewal(Tokens currentTokens) {
    Instant now = clock.get();
    if (Duration.between(lastAccess, now).compareTo(idleInterval) > 0) {
      sleeping.set(true);
      LOGGER.debug("Sleeping...");
    } else {
      Duration delay = nextTokenRefresh(currentTokens, now, MIN_REFRESH_DELAY);
      scheduleTokensRenewal(delay);
    }
  }

  private void scheduleTokensRenewal(Duration delay) {
    if (closing.get()) {
      return;
    }
    LOGGER.debug("Scheduling token refresh in {}", delay);
    try {
      tokenRefreshFuture =
          executor.schedule(this::renewTokens, delay.toMillis(), TimeUnit.MILLISECONDS);
    } catch (RejectedExecutionException e) {
      if (closing.get()) {
        // We raced with close(), ignore
        return;
      }
      maybeWarn("Failed to schedule next token renewal, forcibly sleeping", null);
      sleeping.set(true);
    }
  }

  private void renewTokens() {
    CompletionStage<Tokens> oldTokensStage = currentTokensStage;
    currentTokensStage =
        oldTokensStage
            // try refreshing the current tokens (if they exist)
            .thenApply(this::refreshTokens)
            // if that fails, of if tokens weren't available, try fetching brand-new tokens
            .exceptionally(error -> fetchNewTokens());
    currentTokensStage
        .whenComplete((tokens, error) -> log(error))
        .whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
  }

  private void log(Throwable error) {
    if (error != null) {
      boolean tokensStageCancelled = error instanceof CancellationException && closing.get();
      if (tokensStageCancelled) {
        return;
      }
      if (error instanceof CompletionException) {
        error = error.getCause();
      }
      maybeWarn("Failed to renew tokens", error);
    } else {
      LOGGER.debug("Successfully renewed tokens");
    }
  }

  Tokens fetchNewTokens() {
    LOGGER.debug("Fetching new tokens");
    if (grantType.equals(CONF_NESSIE_OAUTH2_GRANT_TYPE_CLIENT_CREDENTIALS)) {
      ClientCredentialsTokensRequest body =
          ImmutableClientCredentialsTokensRequest.builder().scope(scope).build();
      HttpResponse httpResponse = httpClient.newRequest().postForm(body);
      return httpResponse.readEntity(ClientCredentialsTokensResponse.class);
    } else {
      PasswordTokensRequest body =
          ImmutablePasswordTokensRequest.builder()
              .username(username)
              .password(new String(password, StandardCharsets.UTF_8))
              .scope(scope)
              .build();
      HttpResponse httpResponse = httpClient.newRequest().postForm(body);
      return httpResponse.readEntity(PasswordTokensResponse.class);
    }
  }

  Tokens refreshTokens(Tokens currentTokens) {
    if (currentTokens.getRefreshToken() == null) {
      // no refresh token, try exchanging the access token for a pair of access+refresh tokens
      // (if token exchange is enabled). If that fails, this will throw an exception which will
      // trigger a new token fetch.
      return exchangeTokens(currentTokens);
    }
    if (isAboutToExpire(currentTokens.getRefreshToken())) {
      // refresh token is about to expire, it's not going to be usable anymore:
      // throw an exception to trigger a new token fetch.
      throw new MustFetchNewTokensException("Refresh token is about to expire");
    }
    LOGGER.debug("Refreshing tokens");
    RefreshTokensRequest body =
        ImmutableRefreshTokensRequest.builder()
            .refreshToken(currentTokens.getRefreshToken().getPayload())
            .scope(scope)
            .build();
    HttpResponse httpResponse = httpClient.newRequest().postForm(body);
    return httpResponse.readEntity(RefreshTokensResponse.class);
  }

  Tokens exchangeTokens(Tokens currentToken) {
    if (!tokenExchangeEnabled) {
      throw new MustFetchNewTokensException("Token exchange is disabled");
    }
    LOGGER.debug("Exchanging tokens");
    ImmutableTokensExchangeRequest body =
        ImmutableTokensExchangeRequest.builder()
            .subjectToken(currentToken.getAccessToken().getPayload())
            .subjectTokenType(ACCESS_TOKEN)
            .requestedTokenType(REFRESH_TOKEN)
            .scope(scope)
            .build();
    HttpResponse httpResponse = httpClient.newRequest().postForm(body);
    return httpResponse.readEntity(TokensExchangeResponse.class);
  }

  private boolean isAboutToExpire(Token token) {
    Instant now = clock.get();
    return tokenExpirationTime(now, token, defaultRefreshTokenLifespan)
        .isBefore(now.plus(refreshSafetyWindow));
  }

  /**
   * Compute when the next token refresh should happen, depending on when the access token and the
   * refresh token expire, and on the current time.
   */
  private Duration nextTokenRefresh(Tokens currentTokens, Instant now, Duration minRefreshDelay) {
    if (currentTokens == null) {
      return minRefreshDelay;
    }
    Instant accessExpirationTime =
        tokenExpirationTime(now, currentTokens.getAccessToken(), defaultAccessTokenLifespan);
    Instant refreshExpirationTime =
        tokenExpirationTime(now, currentTokens.getRefreshToken(), defaultRefreshTokenLifespan);
    return shortestDelay(
        now, accessExpirationTime, refreshExpirationTime, refreshSafetyWindow, minRefreshDelay);
  }

  static Duration shortestDelay(
      Instant now,
      Instant accessExpirationTime,
      Instant refreshExpirationTime,
      Duration refreshSafetyWindow,
      Duration minRefreshDelay) {
    Instant expirationTime =
        accessExpirationTime.isBefore(refreshExpirationTime)
            ? accessExpirationTime
            : refreshExpirationTime;
    Duration delay = Duration.between(now, expirationTime).minus(refreshSafetyWindow);
    if (delay.compareTo(minRefreshDelay) < 0) {
      delay = minRefreshDelay;
    }
    return delay;
  }

  static Instant tokenExpirationTime(Instant now, Token token, Duration defaultLifespan) {
    Instant expirationTime = null;
    if (token != null) {
      expirationTime = token.getExpirationTime();
      if (expirationTime == null) {
        try {
          JwtToken jwtToken = JwtToken.parse(token.getPayload());
          expirationTime = jwtToken.getExpirationTime();
        } catch (Exception ignored) {
          // fall through
        }
      }
    }
    if (expirationTime == null) {
      expirationTime = now.plus(defaultLifespan);
    }
    return expirationTime;
  }

  private void checkErrorResponse(ResponseContext responseContext) {
    try {
      Status status = responseContext.getResponseCode();
      if (status.getCode() >= 400) {
        if (!responseContext.isJsonCompatibleResponse()) {
          throw genericError(status);
        }
        InputStream is = responseContext.getErrorStream();
        if (is != null) {
          try {
            ErrorResponse errorResponse = objectMapper.readValue(is, ErrorResponse.class);
            throw new OAuth2Exception(status, errorResponse);
          } catch (IOException ignored) {
            throw genericError(status);
          }
        }
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new HttpClientException(e);
    }
  }

  private void maybeWarn(String message, Throwable error) {
    Instant now = clock.get();
    boolean shouldWarn =
        lastWarn == null || Duration.between(lastWarn, now).compareTo(MIN_WARN_INTERVAL) > 0;
    if (shouldWarn) {
      LOGGER.warn(message, error);
      lastWarn = now;
    } else {
      LOGGER.debug(message, error);
    }
  }

  private static HttpClientException genericError(Status status) {
    return new HttpClientException(
        "OAuth2 server replied with HTTP status code: " + status.getCode());
  }

  static class MustFetchNewTokensException extends RuntimeException {
    public MustFetchNewTokensException(String message) {
      super(message);
    }
  }
}

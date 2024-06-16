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

import static org.projectnessie.client.auth.oauth2.OAuth2Utils.tokenExpirationTime;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
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
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple OAuth2 client that supports fetching new access tokens using the following grant types:
 * client credentials, password, authorization code, and device code.
 *
 * <p>This client also supports refreshing access tokens using both the refresh token method defined
 * in RFC 6749 and the token exchange method defined in RFC 8693.
 *
 * <p>If you don't need to refresh access tokens, you can use the {@link
 * BearerAuthenticationProvider BearerAuthenticationProvider} instead and provide the token to use
 * directly through configuration.
 */
class OAuth2Client implements OAuth2Authenticator, Closeable {

  static final Logger LOGGER = LoggerFactory.getLogger(OAuth2Client.class);
  private static final Duration MIN_WARN_INTERVAL = Duration.ofSeconds(10);

  private final OAuth2ClientConfig config;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;
  private final CompletableFuture<Void> started = new CompletableFuture<>();
  private final CompletableFuture<Void> used = new CompletableFuture<>();
  /* Visible for testing. */ final AtomicBoolean sleeping = new AtomicBoolean();
  private final AtomicBoolean closing = new AtomicBoolean();

  private volatile CompletionStage<Tokens> currentTokensStage;
  private volatile ScheduledFuture<?> tokenRefreshFuture;
  private volatile Instant lastAccess;
  private volatile Instant lastWarn;

  OAuth2Client(OAuth2ClientConfig config) {
    this.config = config;
    httpClient = config.getHttpClient();
    executor = config.getExecutor().orElseGet(() -> new OAuth2TokenRefreshExecutor(config));
    // when user interaction is not required, token fetch can happen immediately upon start();
    // otherwise, it will be deferred until authenticate() is called the first time.
    CompletableFuture<?> ready =
        config.getGrantType().requiresUserInteraction()
            ? CompletableFuture.allOf(started, used)
            : started;
    currentTokensStage =
        ready
            .thenApplyAsync((v) -> fetchNewTokens(), executor)
            .thenApply(this::maybeExchangeTokens);
    currentTokensStage
        .whenComplete((tokens, error) -> log(error))
        .whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
  }

  @Override
  public AccessToken authenticate() {
    if (closing.get()) {
      throw new IllegalStateException("Client is closing");
    }
    used.complete(null);
    Instant now = config.getClock().get();
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
      } else if (cause instanceof HttpClientException) {
        throw (HttpClientException) cause;
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

  @Override
  public void start() {
    lastAccess = config.getClock().get();
    started.complete(null);
  }

  @Override
  public void close() {
    if (closing.compareAndSet(false, true)) {
      LOGGER.debug("[{}] Closing...", config.getClientName());
      try {
        currentTokensStage.toCompletableFuture().cancel(true);
        ScheduledFuture<?> tokenRefreshFuture = this.tokenRefreshFuture;
        if (tokenRefreshFuture != null) {
          tokenRefreshFuture.cancel(true);
        }
        // Only close the executor if it's the default one (not shared).
        if (executor instanceof OAuth2TokenRefreshExecutor) {
          ((OAuth2TokenRefreshExecutor) executor).close();
        }
        // Always close the HTTP client (can't be shared).
        httpClient.close();
      } finally {
        // Cancel this future to invalidate any pending log messages
        used.cancel(true);
        tokenRefreshFuture = null;
      }
      LOGGER.debug("[{}] Closed", config.getClientName());
    }
  }

  @Override
  public OAuth2Authenticator copy() {
    return new OAuth2Client(OAuth2ClientConfig.builder().from(config).build());
  }

  private void wakeUp(Instant now) {
    if (closing.get()) {
      LOGGER.debug("[{}] Not waking up, client is closing", config.getClientName());
      return;
    }
    LOGGER.debug("[{}] Waking up...", config.getClientName());
    Tokens currentTokens = getCurrentTokensIfAvailable();
    Duration delay = nextTokenRefresh(currentTokens, now, Duration.ZERO);
    if (delay.compareTo(config.getMinRefreshSafetyWindow()) < 0) {
      LOGGER.debug("[{}] Refreshing tokens immediately", config.getClientName());
      renewTokens();
    } else {
      LOGGER.debug("[{}] Tokens are still valid", config.getClientName());
      scheduleTokensRenewal(delay);
    }
  }

  private void maybeScheduleTokensRenewal(Tokens currentTokens) {
    if (closing.get()) {
      LOGGER.debug(
          "[{}] Not checking if token renewal is required, client is closing",
          config.getClientName());
      return;
    }
    Instant now = config.getClock().get();
    if (Duration.between(lastAccess, now).compareTo(config.getPreemptiveTokenRefreshIdleTimeout())
        > 0) {
      sleeping.set(true);
      LOGGER.debug("[{}] Sleeping...", config.getClientName());
    } else {
      Duration delay = nextTokenRefresh(currentTokens, now, config.getMinRefreshSafetyWindow());
      scheduleTokensRenewal(delay);
    }
  }

  private void scheduleTokensRenewal(Duration delay) {
    if (closing.get()) {
      LOGGER.debug("[{}] Not scheduling token renewal, client is closing", config.getClientName());
      return;
    }
    LOGGER.debug("[{}] Scheduling token refresh in {}", config.getClientName(), delay);
    try {
      tokenRefreshFuture =
          executor.schedule(this::renewTokens, delay.toMillis(), TimeUnit.MILLISECONDS);
      if (closing.get()) {
        // We raced with close() but the executor wasn't closed yet,
        // so the task was accepted: cancel the future now.
        tokenRefreshFuture.cancel(true);
      }
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
            .exceptionally(error -> fetchNewTokens())
            .thenApply(this::maybeExchangeTokens);
    currentTokensStage
        .whenComplete((tokens, error) -> log(error))
        .whenComplete((tokens, error) -> maybeScheduleTokensRenewal(tokens));
  }

  private void log(Throwable error) {
    if (error != null) {
      if (closing.get()) {
        return;
      }
      if (error instanceof CompletionException) {
        error = error.getCause();
      }
      maybeWarn("Failed to fetch new tokens", error);
    } else {
      LOGGER.debug("[{}] Successfully fetched new tokens", config.getClientName());
    }
  }

  Tokens fetchNewTokens() {
    LOGGER.debug(
        "[{}] Fetching new tokens using {}", config.getClientName(), config.getGrantType());
    try (Flow flow = config.getGrantType().newFlow(config)) {
      return flow.fetchNewTokens(null);
    } finally {
      if (config.getGrantType().requiresUserInteraction()) {
        lastAccess = config.getClock().get();
      }
    }
  }

  Tokens refreshTokens(Tokens currentTokens) {
    if (currentTokens.getRefreshToken() == null) {
      throw new MustFetchNewTokensException("No refresh token available");
    }
    if (isAboutToExpire(currentTokens.getRefreshToken(), config.getDefaultRefreshTokenLifespan())) {
      throw new MustFetchNewTokensException("Refresh token is about to expire");
    }
    LOGGER.debug("[{}] Refreshing tokens", config.getClientName());
    try (Flow flow = GrantType.REFRESH_TOKEN.newFlow(config)) {
      return flow.fetchNewTokens(currentTokens);
    }
  }

  Tokens maybeExchangeTokens(Tokens currentTokens) {
    if (config.getTokenExchangeConfig().isEnabled()) {
      LOGGER.debug("[{}] Exchanging tokens", config.getClientName());
      try (Flow flow = GrantType.TOKEN_EXCHANGE.newFlow(config)) {
        return flow.fetchNewTokens(currentTokens);
      }
    }
    return currentTokens;
  }

  private boolean isAboutToExpire(Token token, Duration defaultLifespan) {
    Instant now = config.getClock().get();
    Instant expirationTime = tokenExpirationTime(now, token, defaultLifespan);
    return expirationTime.isBefore(now.plus(config.getRefreshSafetyWindow()));
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
        tokenExpirationTime(
            now, currentTokens.getAccessToken(), config.getDefaultAccessTokenLifespan());
    Instant refreshExpirationTime =
        tokenExpirationTime(
            now, currentTokens.getRefreshToken(), config.getDefaultRefreshTokenLifespan());
    return OAuth2Utils.shortestDelay(
        now,
        accessExpirationTime,
        refreshExpirationTime,
        config.getRefreshSafetyWindow(),
        minRefreshDelay);
  }

  private void maybeWarn(String message, Throwable error) {
    Instant now = config.getClock().get();
    boolean shouldWarn =
        lastWarn == null || Duration.between(lastWarn, now).compareTo(MIN_WARN_INTERVAL) > 0;
    if (shouldWarn) {
      // defer logging until the client is used to avoid confusing log messages appearing
      // before the client is actually used
      if (error instanceof HttpClientException) {
        used.thenRun(() -> LOGGER.warn("{}: {}", message, error.toString()));
      } else {
        used.thenRun(() -> LOGGER.warn(message, error));
      }
      lastWarn = now;
    } else {
      LOGGER.debug(message, error);
    }
  }
}

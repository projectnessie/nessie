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

import java.io.PrintStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.projectnessie.client.http.HttpClientException;

/**
 * An implementation of the <a href="https://datatracker.ietf.org/doc/html/rfc8628">Device
 * Authorization Grant</a> flow.
 */
class DeviceCodeFlow extends AbstractFlow {

  static final String MSG_PREFIX = "[nessie-oauth2-client] ";

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(DeviceCodeFlow.class);

  private final PrintStream console;
  private final Duration flowTimeout;

  /**
   * A future that will complete when fresh tokens are eventually obtained after polling the token
   * endpoint.
   */
  private final CompletableFuture<Tokens> tokensFuture = new CompletableFuture<>();

  /**
   * A future that will complete when the close() method is called. It is used merely to avoid
   * closing resources multiple times. Its completion stops the internal polling loop and its
   * executor.
   */
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  /** The executor that is used to periodically poll the token endpoint. */
  private final ScheduledExecutorService executor;

  private volatile Duration pollInterval;
  private volatile Future<?> pollFuture;

  DeviceCodeFlow(OAuth2ClientConfig config) {
    super(config);
    this.console = config.getConsole();
    flowTimeout = config.getDeviceCodeFlowTimeout();
    pollInterval = config.getDeviceCodeFlowPollInterval();
    closeFuture.thenRun(this::doClose);
    LOGGER.debug("Device Code Flow: started");
    executor = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public void close() {
    closeFuture.complete(null);
  }

  private void doClose() {
    LOGGER.debug("Device Code Flow: closing");
    executor.shutdownNow();
    pollFuture = null;
    // don't close the HTTP client nor the console, they are not ours
  }

  private void abort() {
    Future<?> pollFuture = this.pollFuture;
    if (pollFuture != null) {
      pollFuture.cancel(true);
    }
    tokensFuture.cancel(true);
  }

  @Override
  public Tokens fetchNewTokens(@Nullable Tokens currentTokens) {
    DeviceCodeResponse response = invokeDeviceAuthEndpoint();
    checkPollInterval(response.getIntervalSeconds());
    console.println();
    console.println(MSG_PREFIX + "======= Nessie authentication required =======");
    console.println(MSG_PREFIX + "Browse to the following URL:");
    console.println(MSG_PREFIX + response.getVerificationUri());
    console.println(MSG_PREFIX + "And enter the code:");
    console.println(MSG_PREFIX + response.getUserCode());
    printExpirationNotice(response.getExpiresInSeconds());
    console.println();
    console.flush();
    pollFuture = executor.submit(() -> pollForNewTokens(response.getDeviceCode()));
    try {
      return tokensFuture.get(flowTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOGGER.error("Timed out waiting for user to authorize device.");
      abort();
      throw new RuntimeException("Timed out waiting for user to authorize device", e);
    } catch (InterruptedException e) {
      abort();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      abort();
      Throwable cause = e.getCause();
      LOGGER.error("Authentication failed: " + cause.getMessage());
      if (cause instanceof HttpClientException) {
        throw (HttpClientException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  private void checkPollInterval(Integer serverPollInterval) {
    if (!config.ignoreDeviceCodeFlowServerPollInterval()
        && serverPollInterval != null
        && serverPollInterval > pollInterval.getSeconds()) {
      LOGGER.debug(
          "Device Code Flow: server requested minimum poll interval of {} seconds",
          serverPollInterval);
      pollInterval = Duration.ofSeconds(serverPollInterval);
    }
  }

  private void printExpirationNotice(int seconds) {
    String exp;
    if (seconds < 60) {
      exp = seconds + " seconds";
    } else if (seconds % 60 == 0) {
      exp = seconds / 60 + " minutes";
    } else {
      exp = seconds / 60 + " minutes and " + seconds % 60 + " seconds";
    }
    console.println(MSG_PREFIX + "(The code will expire in " + exp + ")");
  }

  private void pollForNewTokens(String deviceCode) {
    try {
      LOGGER.debug("Device Code Flow: polling for new tokens");
      DeviceCodeTokenRequest.Builder request =
          DeviceCodeTokenRequest.builder().deviceCode(deviceCode);
      Tokens tokens = invokeTokenEndpoint(request, DeviceCodeTokenResponse.class);
      LOGGER.debug("Device Code Flow: new tokens received");
      tokensFuture.complete(tokens);
    } catch (OAuth2Exception e) {
      switch (e.getErrorCode()) {
        case "authorization_pending":
          LOGGER.debug("Device Code Flow: waiting for authorization to complete");
          pollFuture =
              executor.schedule(
                  () -> pollForNewTokens(deviceCode),
                  pollInterval.toMillis(),
                  TimeUnit.MILLISECONDS);
          return;
        case "slow_down":
          LOGGER.debug("Device Code Flow: server requested to slow down");
          Duration pollInterval = this.pollInterval;
          if (!config.ignoreDeviceCodeFlowServerPollInterval()) {
            pollInterval = pollInterval.plus(pollInterval);
            this.pollInterval = pollInterval;
          }
          pollFuture =
              executor.schedule(
                  () -> pollForNewTokens(deviceCode),
                  pollInterval.toMillis(),
                  TimeUnit.MILLISECONDS);
          return;
        case "access_denied":
        case "expired_token":
        default:
          tokensFuture.completeExceptionally(e);
      }
    } catch (Exception e) {
      tokensFuture.completeExceptionally(e);
    }
  }
}

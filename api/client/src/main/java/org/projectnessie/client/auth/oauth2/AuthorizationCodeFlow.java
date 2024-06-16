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

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.impl.HttpUtils;
import org.projectnessie.client.http.impl.UriBuilder;

/**
 * An implementation of the <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-4.1">Authorization Code Grant</a>
 * flow.
 */
class AuthorizationCodeFlow extends AbstractFlow {

  static final String CONTEXT_PATH = "/nessie-client/auth";
  static final String MSG_PREFIX = "[nessie-oauth2-client] ";

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(AuthorizationCodeFlow.class);

  private static final String HTML_TEMPLATE_OK =
      "<html><body><h1>Authentication successful</h1><p>You can close this page now.</p></body></html>";
  private static final String HTML_TEMPLATE_FAILED =
      "<html><body><h1>Authentication failed</h1><p>Could not obtain access token: %s</p></body></html>";

  private static final int STATE_LENGTH = 16;

  private final PrintStream console;
  private final String state;
  private final HttpServer server;
  private final String redirectUri;
  private final URI authorizationUri;
  private final Duration flowTimeout;

  /**
   * A future that will complete when the redirect URI is called for the first time. It will then
   * trigger the code extraction then the token fetching. Subsequent calls to the redirect URI will
   * not trigger any action. Note that the response to the redirect URI will be delayed until the
   * tokens are received.
   */
  private final CompletableFuture<HttpExchange> redirectUriFuture = new CompletableFuture<>();

  /**
   * A future that will complete when the tokens are received in exchange for the authorization
   * code. Its completion will release all pending responses to the redirect URI. If the redirect
   * URI is called again after the tokens are received, the response will be immediate.
   */
  private final CompletableFuture<Tokens> tokensFuture;

  /**
   * A future that will complete when the close() method is called. It is used merely to avoid
   * closing resources multiple times. Its completion stops the internal HTTP server.
   */
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

  /**
   * A phaser that will delay closing the internal HTTP server until all inflight requests have been
   * processed. It is used to avoid closing the server prematurely and leaving the user's browser
   * with an aborted HTTP request.
   */
  private final Phaser inflightRequestsPhaser = new Phaser(1);

  AuthorizationCodeFlow(OAuth2ClientConfig config) {
    super(config);
    this.console = config.getConsole();
    this.flowTimeout = config.getAuthorizationCodeFlowTimeout();
    tokensFuture =
        redirectUriFuture
            .thenApply(this::extractAuthorizationCode)
            .thenApply(this::fetchNewTokens)
            .whenComplete(this::log);
    closeFuture.thenRun(this::doClose);
    server =
        createServer(config.getAuthorizationCodeFlowWebServerPort().orElse(0), this::doRequest);
    state = OAuth2Utils.randomAlphaNumString(STATE_LENGTH);
    redirectUri =
        String.format("http://localhost:%d%s", server.getAddress().getPort(), CONTEXT_PATH);
    URI authEndpoint = config.getResolvedAuthEndpoint();
    authorizationUri =
        new UriBuilder(authEndpoint.resolve("/"))
            .path(authEndpoint.getPath())
            .queryParam("response_type", "code")
            .queryParam("client_id", config.getClientId())
            .queryParam(
                "scope", config.getScopes().stream().reduce((a, b) -> a + " " + b).orElse(null))
            .queryParam("redirect_uri", redirectUri)
            .queryParam("state", state)
            .build();
    LOGGER.debug("Authorization Code Flow: started, redirect URI: {}", redirectUri);
  }

  @Override
  public void close() {
    closeFuture.complete(null);
  }

  private void doClose() {
    inflightRequestsPhaser.arriveAndAwaitAdvance();
    LOGGER.debug("Authorization Code Flow: closing");
    server.stop(0);
    // don't close the HTTP client nor the console, they are not ours
  }

  private void abort() {
    tokensFuture.cancel(true);
    redirectUriFuture.cancel(true);
  }

  @Override
  public Tokens fetchNewTokens(@Nullable Tokens ignored) {
    console.println();
    console.println(MSG_PREFIX + "======= Nessie authentication required =======");
    console.println(MSG_PREFIX + "Browse to the following URL to continue:");
    console.println(MSG_PREFIX + authorizationUri);
    console.println();
    console.flush();
    try {
      return tokensFuture.get(flowTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOGGER.error("Timed out waiting for authorization code.");
      abort();
      throw new RuntimeException("Timed out waiting waiting for authorization code", e);
    } catch (InterruptedException e) {
      abort();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      abort();
      Throwable cause = e.getCause();
      LOGGER.error("Authentication failed: {}", cause.toString());
      if (cause instanceof HttpClientException) {
        throw (HttpClientException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  /**
   * Handle the incoming HTTP request to the redirect URI. Since we are using the default executor,
   * which is a synchronous one, the very first invocation of this method will block the HTTP
   * server's dispatcher thread, until the authorization code is extracted and exchanged for tokens.
   * Subsequent requests will be processed immediately. The response to the request will be delayed
   * until the tokens are received.
   */
  private void doRequest(HttpExchange exchange) {
    LOGGER.debug("Authorization Code Flow: received request");
    inflightRequestsPhaser.register();
    redirectUriFuture.complete(exchange); // will trigger the token fetching the first time
    tokensFuture
        .handle((tokens, error) -> doResponse(exchange, error))
        .whenComplete((v, error) -> exchange.close())
        .whenComplete((v, error) -> inflightRequestsPhaser.arriveAndDeregister());
  }

  /** Send the response to the incoming HTTP request to the redirect URI. */
  private Void doResponse(HttpExchange exchange, Throwable error) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Authorization Code Flow: sending response, error: {}",
          error == null ? "none" : error.toString());
    }
    try {
      if (error == null) {
        writeResponse(exchange, HTTP_OK, HTML_TEMPLATE_OK);
      } else {
        writeResponse(exchange, HTTP_UNAUTHORIZED, HTML_TEMPLATE_FAILED, error.toString());
      }
    } catch (IOException e) {
      LOGGER.debug("Authorization Code Flow: error writing response", e);
    }
    return null;
  }

  private String extractAuthorizationCode(HttpExchange exchange) {
    LOGGER.debug("Authorization Code Flow: extracting code");
    Map<String, String> params = HttpUtils.parseQueryString(exchange.getRequestURI().getQuery());
    if (!state.equals(params.get("state"))) {
      throw new IllegalArgumentException("Missing or invalid state");
    }
    String code = params.get("code");
    if (code == null || code.isEmpty()) {
      throw new IllegalArgumentException("Missing authorization code");
    }
    return code;
  }

  private Tokens fetchNewTokens(String code) {
    LOGGER.debug("Authorization Code Flow: fetching new tokens");
    AuthorizationCodeTokenRequest.Builder request =
        AuthorizationCodeTokenRequest.builder().code(code).redirectUri(redirectUri);
    Tokens tokens = invokeTokenEndpoint(request, AuthorizationCodeTokenResponse.class);
    LOGGER.debug("Authorization Code Flow: new tokens received");
    return tokens;
  }

  private void log(Tokens tokens, Throwable error) {
    if (LOGGER.isDebugEnabled()) {
      if (error == null) {
        LOGGER.debug("Authorization Code Flow: tokens received");
      } else {
        LOGGER.debug("Authorization Code Flow: error fetching tokens: {}", error.toString());
      }
    }
  }

  private static HttpServer createServer(int port, HttpHandler handler) {
    final HttpServer server;
    try {
      server = HttpServer.create();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    server.createContext(CONTEXT_PATH, handler);
    InetAddress local = InetAddress.getLoopbackAddress();
    try {
      server.bind(new InetSocketAddress(local, port), 0);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    server.start();
    return server;
  }

  private static void writeResponse(
      HttpExchange exchange, int status, String htmlTemplate, Object... args) throws IOException {
    String html = String.format(htmlTemplate, args);
    exchange.getResponseHeaders().add("Content-Type", "text/html");
    exchange.sendResponseHeaders(status, html.length());
    exchange.getResponseBody().write(html.getBytes(StandardCharsets.UTF_8));
  }
}

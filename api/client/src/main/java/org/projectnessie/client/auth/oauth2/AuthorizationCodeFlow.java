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
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpRequest;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.impl.HttpUtils;
import org.projectnessie.client.http.impl.UriBuilder;

class AuthorizationCodeFlow implements AutoCloseable {

  static final String CONTEXT_PATH = "/nessie-client/auth";
  static final String MSG_PREFIX = "[nessie-oauth2-client] ";

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(AuthorizationCodeFlow.class);

  private static final String HTML_TEMPLATE_OK =
      "<html><body><h1>Authentication successful</h1><p>You can close this page now.</p></body></html>";
  private static final String HTML_TEMPLATE_FAILED =
      "<html><body><h1>Authentication failed</h1><p>Could not obtain access token: %s</p></body></html>";

  private static final int STATE_LENGTH = 16;

  private final OAuth2ClientParams params;
  private final HttpClient httpClient;
  private final PrintStream console;
  private final String state;
  private final HttpServer server;
  private final String redirectUri;
  private final URI authorizationUri;
  private final Duration flowTimeout;

  private final CompletableFuture<HttpExchange> requestFuture = new CompletableFuture<>();
  private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
  private final CompletableFuture<String> authCodeFuture;
  private final CompletableFuture<Tokens> tokensFuture;

  private final Phaser inflightRequestsPhaser = new Phaser(1);

  AuthorizationCodeFlow(OAuth2ClientParams params, HttpClient httpClient) {
    this(params, httpClient, System.out);
  }

  /** Constructor used for testing. */
  AuthorizationCodeFlow(OAuth2ClientParams params, PrintStream console) {
    this(params, params.getHttpClient().build(), console);
  }

  private AuthorizationCodeFlow(
      OAuth2ClientParams params, HttpClient httpClient, PrintStream console) {
    this.params = params;
    this.httpClient = httpClient;
    this.console = console;
    this.flowTimeout = params.getAuthorizationCodeFlowTimeout();
    authCodeFuture = requestFuture.thenApply(this::extractAuthorizationCode);
    tokensFuture = authCodeFuture.thenApply(this::fetchNewTokens);
    closeFuture.thenRun(this::doClose);
    server =
        createServer(params.getAuthorizationCodeFlowWebServerPort().orElse(-1), this::doRequest);
    state = OAuth2Utils.randomAlphaNumString(STATE_LENGTH);
    redirectUri =
        String.format("http://localhost:%d%s", server.getAddress().getPort(), CONTEXT_PATH);
    URI authEndpoint = params.getAuthEndpoint().orElseThrow(IllegalStateException::new);
    authorizationUri =
        new UriBuilder(authEndpoint.resolve("/"))
            .path(authEndpoint.getPath())
            .queryParam("response_type", "code")
            .queryParam("client_id", params.getClientId())
            .queryParam("scope", params.getScope().orElse(null))
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
    // don't close the console, it's not ours
  }

  private void abort() {
    tokensFuture.cancel(true);
    authCodeFuture.cancel(true);
    requestFuture.cancel(true);
  }

  public Tokens fetchNewTokens() {
    console.println();
    console.println(MSG_PREFIX + "======= Nessie authentication required =======");
    console.println(MSG_PREFIX + "Browse to the following URL to continue:");
    console.println(MSG_PREFIX + authorizationUri);
    console.println();
    console.flush();
    try {
      return tokensFuture.get(flowTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      LOGGER.error(MSG_PREFIX + "Timed out waiting for authorization code.");
      abort();
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      abort();
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      abort();
      Throwable cause = e.getCause();
      LOGGER.error(MSG_PREFIX + "Authentication failed: " + cause.getMessage());
      if (cause instanceof HttpClientException) {
        throw (HttpClientException) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  private void doRequest(HttpExchange exchange) {
    LOGGER.debug("Authorization Code Flow: received request");
    inflightRequestsPhaser.register();
    requestFuture.complete(exchange);
    tokensFuture
        .handle((tokens, error) -> doResponse(exchange, error))
        .whenComplete((v, error) -> exchange.close())
        .whenComplete((v, error) -> inflightRequestsPhaser.arriveAndDeregister());
  }

  private Void doResponse(HttpExchange exchange, Throwable error) {
    LOGGER.debug("Authorization Code Flow: sending response");
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
    AuthorizationCodeTokensRequest body =
        ImmutableAuthorizationCodeTokensRequest.builder()
            .code(code)
            .redirectUri(redirectUri)
            .clientId(params.getClientId())
            .scope(params.getScope().orElse(null))
            .build();
    HttpRequest request = httpClient.newRequest().path(params.getTokenEndpoint().getPath());
    HttpResponse response = request.postForm(body);
    Tokens tokens = response.readEntity(AuthorizationCodeTokensResponse.class);
    LOGGER.debug("Authorization Code Flow: new tokens received");
    return tokens;
  }

  private static HttpServer createServer(int port, HttpHandler handler) {
    final HttpServer server;
    try {
      server = HttpServer.create();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    server.createContext(CONTEXT_PATH, handler);
    bind(server, port);
    server.start();
    return server;
  }

  private static void bind(HttpServer server, int port) {
    boolean useAnyPort = port == -1;
    int maxAttempts = useAnyPort ? 3 : 1;
    int attempt = 1;
    IOException ioe = null;
    while (attempt <= maxAttempts) {
      try {
        InetAddress local = InetAddress.getLoopbackAddress();
        if (useAnyPort) {
          try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
          }
        }
        server.bind(new InetSocketAddress(local, port), 0);
        return;
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
      attempt++;
    }
    throw new UncheckedIOException(ioe);
  }

  private static void writeResponse(
      HttpExchange exchange, int status, String htmlTemplate, Object... args) throws IOException {
    String html = String.format(htmlTemplate, args);
    exchange.getResponseHeaders().add("Content-Type", "text/html");
    exchange.sendResponseHeaders(status, html.length());
    exchange.getResponseBody().write(html.getBytes(StandardCharsets.UTF_8));
  }
}

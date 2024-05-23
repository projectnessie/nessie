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

import static java.net.HttpURLConnection.HTTP_MOVED_PERM;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_SEE_OTHER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class InteractiveResourceOwnerEmulator implements ResourceOwnerEmulator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(InteractiveResourceOwnerEmulator.class);

  private static final AtomicInteger COUNTER = new AtomicInteger(1);

  protected final String username;
  protected final String password;
  private final SSLContext sslContext;

  private final ExecutorService executor;

  private final PipedOutputStream pipeOut = new PipedOutputStream();
  private final PipedInputStream pipeIn = new PipedInputStream(pipeOut);
  private final PrintStream consoleOut = new PrintStream(pipeOut, true, "UTF-8");
  private final BufferedReader consoleIn = new BufferedReader(new InputStreamReader(pipeIn, UTF_8));
  private final PrintStream standardOut;

  private volatile boolean replaceSystemOut;
  private volatile boolean closing;
  private volatile Throwable error;
  private volatile URI baseUri;
  private volatile Consumer<URI> authUrlListener;
  private volatile Consumer<Throwable> errorListener;
  private volatile Runnable completionListener;

  public InteractiveResourceOwnerEmulator(String username, String password) throws IOException {
    this(username, password, null);
  }

  public InteractiveResourceOwnerEmulator(String username, String password, SSLContext sslContext)
      throws IOException {
    this.username = username;
    this.password = password;
    this.sslContext = sslContext;
    executor =
        Executors.newFixedThreadPool(
            2, r -> new Thread(r, getClass().getSimpleName() + COUNTER.getAndIncrement()));
    standardOut = System.out;
    executor.submit(this::readConsole);
  }

  @Override
  public PrintStream getConsole() {
    return consoleOut;
  }

  public void setAuthServerBaseUri(URI baseUri) {
    this.baseUri = baseUri;
  }

  public void replaceSystemOut() {
    this.replaceSystemOut = true;
    System.setOut(consoleOut);
  }

  public void setErrorListener(Consumer<Throwable> callback) {
    this.errorListener = callback;
  }

  public void setAuthUrlListener(Consumer<URI> callback) {
    this.authUrlListener = callback;
  }

  public void setCompletionListener(Runnable listener) {
    this.completionListener = listener;
  }

  private void readConsole() {
    try {
      String line;
      while ((line = consoleIn.readLine()) != null) {
        standardOut.println(line);
        standardOut.flush();
        Runnable flow = processLine(line);
        if (flow != null) {
          executor.submit(flow);
        }
      }
    } catch (IOException ignored) {
      // Expected: consoleIn.readLine() throws an IOException when closing
    } catch (Throwable t) {
      recordFailure(t);
    }
  }

  protected abstract Runnable processLine(String line);

  protected void notifyFlowCompleted() {
    Runnable listener = completionListener;
    if (listener != null) {
      listener.run();
    }
  }

  protected URI extractAuthUrl(String line) {
    URI authUrl;
    try {
      authUrl = new URI(line.substring(line.indexOf("http")));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    Consumer<URI> listener = authUrlListener;
    if (listener != null) {
      listener.accept(authUrl);
    }
    return authUrl;
  }

  /** Emulate user logging in to the authorization server. */
  protected abstract URI login(URI loginPageUrl, Set<String> cookies) throws Exception;

  protected String getHtmlPage(URI url, Set<String> cookies) throws Exception {
    HttpURLConnection conn = openConnection(url);
    conn.setRequestMethod("GET");
    writeCookies(conn, cookies);
    String html = readBody(conn);
    assertThat(conn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(conn, cookies);
    conn.disconnect();
    return html;
  }

  protected void recordFailure(Throwable t) {
    if (!closing) {
      Consumer<Throwable> errorListener = this.errorListener;
      if (errorListener != null) {
        errorListener.accept(t);
      }
      Throwable e = error;
      if (e == null) {
        error = t;
      } else {
        e.addSuppressed(t);
      }
    }
  }

  /**
   * Open a connection to the given URL, optionally replacing hostname and port with those actually
   * accessible by this client; this is necessary because the auth server may be sending URLs with a
   * hostname + port address that is only accessible within a Docker network, e.g. keycloak:8080.
   */
  protected HttpURLConnection openConnection(URI url) throws Exception {
    // See https://github.com/projectnessie/nessie/issues/7918
    return openConnection(url, "text/html, *; q=.2, */*; q=.2");
  }

  protected HttpURLConnection openConnection(URI url, String accept) throws Exception {
    HttpURLConnection conn;
    if (baseUri == null || baseUri.getHost().equals(url.getHost())) {
      conn = (HttpURLConnection) url.toURL().openConnection();
    } else {
      URI transformed =
          new URI(
              baseUri.getScheme(),
              null,
              baseUri.getHost(),
              baseUri.getPort(),
              url.getPath(),
              url.getQuery(),
              null);
      conn = (HttpURLConnection) transformed.toURL().openConnection();
    }
    conn.addRequestProperty("Accept", accept);
    if (sslContext != null && conn instanceof HttpsURLConnection) {
      ((HttpsURLConnection) conn).setSSLSocketFactory(sslContext.getSocketFactory());
    }
    return conn;
  }

  @Override
  public void close() throws Exception {
    closing = true;
    if (replaceSystemOut) {
      System.setOut(standardOut);
    }
    try {
      // close writer first to signal end of input to reader
      consoleOut.close();
      consoleIn.close();
    } catch (IOException e) {
      LOGGER.warn("Error closing console streams", e);
    }
    MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
    Throwable t = error;
    if (t != null) {
      if (t instanceof Exception) {
        throw (Exception) t;
      } else if (t instanceof Error) {
        throw (Error) t;
      } else {
        throw new RuntimeException(t);
      }
    }
  }

  protected static void postForm(
      HttpURLConnection conn, Map<String, String> data, Set<String> cookies) throws IOException {
    conn.setRequestMethod("POST");
    conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    writeCookies(conn, cookies);
    conn.setDoOutput(true);
    try (OutputStream out = conn.getOutputStream()) {
      for (Iterator<String> iterator = data.keySet().iterator(); iterator.hasNext(); ) {
        String name = iterator.next();
        String value = data.get(name);
        out.write(URLEncoder.encode(name, "UTF-8").getBytes(UTF_8));
        out.write('=');
        out.write(URLEncoder.encode(value, "UTF-8").getBytes(UTF_8));
        if (iterator.hasNext()) {
          out.write('&');
        }
      }
    }
  }

  protected static void postJson(HttpURLConnection conn, String json, Set<String> cookies)
      throws IOException {
    conn.setRequestMethod("POST");
    conn.addRequestProperty("Content-Type", "application/json; charset=utf-8");
    writeCookies(conn, cookies);
    conn.setDoOutput(true);
    try (OutputStream out = conn.getOutputStream()) {
      out.write(json.getBytes(UTF_8));
    }
  }

  protected static String readBody(HttpURLConnection conn) throws IOException {
    String html;
    try (InputStream is = conn.getInputStream()) {
      html =
          new BufferedReader(new InputStreamReader(is, UTF_8))
              .lines()
              .collect(Collectors.joining("\n"));
    }
    return html;
  }

  protected static URI readRedirectUrl(HttpURLConnection conn, Set<String> cookies)
      throws Exception {
    conn.setInstanceFollowRedirects(false);
    int responseCode = conn.getResponseCode();
    assertThat(responseCode).isIn(HTTP_MOVED_PERM, HTTP_MOVED_TEMP, HTTP_SEE_OTHER);
    String location = conn.getHeaderField("Location");
    assertThat(location).isNotNull();
    readCookies(conn, cookies);
    LOGGER.info("Redirected to: {}", location);
    return URI.create(location);
  }

  protected static void readCookies(HttpURLConnection conn, Set<String> cookies) {
    List<String> cks = conn.getHeaderFields().get("Set-Cookie");
    if (cks != null) {
      cookies.addAll(cks);
    }
  }

  protected static void writeCookies(HttpURLConnection conn, Set<String> cookies) {
    for (String c : cookies) {
      conn.addRequestProperty("Cookie", c);
    }
  }
}

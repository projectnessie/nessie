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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.HttpUtils;

/**
 * Emulates a resource owner (user) that "reads" the console (system out) and "uses" their browser
 * to authenticate with a Keycloak authorization server.
 */
public class ResourceOwnerEmulator implements AutoCloseable {

  public enum CodeInputMethod {
    CALLBACK_URL,
    MANUAL
  }

  private static final Pattern KC_AUTH_FORM_PATTERN =
      Pattern.compile("<form.*id=\"kc-form-login\".*action=\"([^\"]+)\".*>");

  private final CodeInputMethod codeInputMethod;
  private final boolean replaceSystemInOut;
  private final boolean expectLoginPage;
  private final byte[] loginData;
  private final ExecutorService executor;

  private final PrintStream standardOut;
  private final PrintStream consoleOut;
  private final BufferedReader consoleOutReader;

  private final InputStream standardIn;
  private final InputStream consoleIn;
  private final PrintStream consoleInWriter;

  private volatile boolean closing;
  private volatile Throwable error;
  private volatile Consumer<URL> authUrlListener;
  private volatile Consumer<Throwable> errorListener;
  private volatile String authorizationCode;
  private volatile int expectedCallbackStatus = HTTP_OK;

  public ResourceOwnerEmulator() throws IOException {
    this(null, null);
  }

  public ResourceOwnerEmulator(CodeInputMethod codeInputMethod) throws IOException {
    this(null, null, codeInputMethod);
  }

  public ResourceOwnerEmulator(String username, String password) throws IOException {
    this(username, password, CodeInputMethod.CALLBACK_URL);
  }

  public ResourceOwnerEmulator(String username, String password, CodeInputMethod codeInputMethod)
      throws IOException {
    this(username, password, codeInputMethod, true);
  }

  public ResourceOwnerEmulator(
      String username, String password, CodeInputMethod codeInputMethod, boolean replaceSystemInOut)
      throws IOException {
    this.codeInputMethod = codeInputMethod;
    this.replaceSystemInOut = replaceSystemInOut;
    this.expectLoginPage = username != null && password != null;
    loginData =
        expectLoginPage
            ? ("username="
                    + URLEncoder.encode(username, "UTF-8")
                    + "&"
                    + "password="
                    + URLEncoder.encode(password, "UTF-8")
                    + "&credentialId=")
                .getBytes(UTF_8)
            : null;
    executor = Executors.newFixedThreadPool(2);
    PipedOutputStream pipeOut = new PipedOutputStream();
    consoleOut = new PrintStream(pipeOut);
    consoleOutReader =
        new BufferedReader(new InputStreamReader(new PipedInputStream(pipeOut), UTF_8));
    PipedInputStream pipeIn = new PipedInputStream();
    consoleIn = new BufferedInputStream(pipeIn);
    consoleInWriter = new PrintStream(new PipedOutputStream(pipeIn));
    standardOut = System.out;
    standardIn = System.in;
    if (replaceSystemInOut) {
      System.setOut(consoleOut);
      System.setIn(consoleIn);
    }
    executor.submit(this::readConsole);
  }

  public PrintStream getConsoleOut() {
    return consoleOut;
  }

  public InputStream getConsoleIn() {
    return consoleIn;
  }

  public void overrideAuthorizationCode(String code, Status expectedStatus) {
    authorizationCode = code;
    expectedCallbackStatus = expectedStatus.getCode();
  }

  public void setErrorListener(Consumer<Throwable> callback) {
    this.errorListener = callback;
  }

  public void setAuthUrlListener(Consumer<URL> callback) {
    this.authUrlListener = callback;
  }

  private void readConsole() {
    try {
      String line;
      while ((line = consoleOutReader.readLine()) != null) {
        standardOut.println(line);
        if (line.startsWith(AuthorizationCodeFlow.MSG_PREFIX) && line.contains("http")) {
          URL authUrl = new URL(line.substring(line.indexOf("http")));
          Consumer<URL> listener = authUrlListener;
          if (listener != null) {
            listener.accept(authUrl);
          }
          executor.submit(() -> useBrowser(authUrl));
        }
      }
    } catch (IOException ignored) {
      // Expected when closing the console
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /**
   * Emulate user browsing to the authorization URL printed on the console, then following the
   * instructions and optionally logging in with their credentials.
   */
  private void useBrowser(URL authUrl) {
    try {
      HttpURLConnection authUrlConn = (HttpURLConnection) authUrl.openConnection();
      authUrlConn.setRequestMethod("GET");
      HttpURLConnection conn = expectLoginPage ? doLogin(authUrlConn) : authUrlConn;
      conn.setInstanceFollowRedirects(false);
      int status = conn.getResponseCode();
      assertThat(status).isIn(HTTP_MOVED_TEMP, HTTP_MOVED_PERM);
      String location = conn.getHeaderField("Location");
      URL callbackUrl = new URL(location);
      assertThat(callbackUrl)
          .hasPath(AuthorizationCodeFlow.CONTEXT_PATH)
          .hasParameter("code")
          .hasParameter("state");
      if (codeInputMethod == CodeInputMethod.CALLBACK_URL) {
        invokeCallbackUrl(callbackUrl);
      } else {
        enterCodeManually(callbackUrl);
      }
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /** Emulate user logging in to the authorization server. Note: this is Keycloak-specific. */
  private HttpURLConnection doLogin(HttpURLConnection authUrlConn) throws IOException {
    String html;
    try (InputStream is = authUrlConn.getInputStream()) {
      html =
          new BufferedReader(new InputStreamReader(is, UTF_8))
              .lines()
              .collect(Collectors.joining("\n"));
    }
    Matcher matcher = KC_AUTH_FORM_PATTERN.matcher(html);
    assertThat(matcher.find()).isTrue();
    URL loginUrl = new URL(matcher.group(1));
    // replace hostname and port with those from the auth URL;
    // this is necessary because the auth server may be sending
    // the login form to its configured frontend hostname + port,
    // which, usually when using Docker, is something like keycloak:8080
    loginUrl =
        new URL(
            loginUrl.getProtocol(),
            authUrlConn.getURL().getHost(),
            authUrlConn.getURL().getPort(),
            loginUrl.getFile());
    List<String> cookies = authUrlConn.getHeaderFields().get("Set-Cookie");
    // send login form
    HttpURLConnection loginUrlConn = (HttpURLConnection) loginUrl.openConnection();
    loginUrlConn.setRequestMethod("POST");
    for (String c : cookies) {
      loginUrlConn.addRequestProperty("Cookie", c);
    }
    loginUrlConn.setDoOutput(true);
    loginUrlConn.getOutputStream().write(loginData);
    loginUrlConn.getOutputStream().close();
    return loginUrlConn;
  }

  /** Emulate browser being redirected to callback URL. */
  private void invokeCallbackUrl(URL callbackUrl) throws IOException {
    String code = authorizationCode;
    if (code != null) {
      Map<String, String> params = HttpUtils.parseQueryString(callbackUrl.getQuery());
      callbackUrl =
          new URL(
              callbackUrl.getProtocol(),
              callbackUrl.getHost(),
              callbackUrl.getPort(),
              callbackUrl.getPath()
                  + "?code="
                  + URLEncoder.encode(code, "UTF-8")
                  + "&state="
                  + params.get("state"));
    }
    HttpURLConnection con = (HttpURLConnection) callbackUrl.openConnection();
    con.setRequestMethod("GET");
    int status = con.getResponseCode();
    assertThat(status).isEqualTo(expectedCallbackStatus);
  }

  /** Emulate user entering authorization code manually on the console. */
  private void enterCodeManually(URL callbackUrl) throws IOException {
    String code = authorizationCode;
    if (code == null) {
      Map<String, String> params = HttpUtils.parseQueryString(callbackUrl.getQuery());
      code = params.get("code");
    }
    consoleInWriter.println(code);
  }

  private void recordFailure(Throwable t) {
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

  @Override
  public void close() throws Exception {
    closing = true;
    if (replaceSystemInOut) {
      System.setOut(standardOut);
      System.setIn(standardIn);
    }
    executor.shutdownNow();
    consoleOutReader.close();
    consoleOut.close();
    consoleInWriter.close();
    consoleIn.close();
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
}

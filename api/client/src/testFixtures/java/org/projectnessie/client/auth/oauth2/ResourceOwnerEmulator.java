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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates a resource owner (user) that "reads" the console (system out) and "uses" their browser
 * to authenticate with a Keycloak authorization server.
 */
public class ResourceOwnerEmulator implements AutoCloseable {

  /**
   * Dummy factory method to circumvent class loading issues when instantiating this class from
   * within a Quarkus test.
   */
  public static ResourceOwnerEmulator forAuthorizationCode() throws IOException {
    return new ResourceOwnerEmulator(GrantType.AUTHORIZATION_CODE);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceOwnerEmulator.class);

  private static final Pattern FORM_ACTION_PATTERN =
      Pattern.compile("<form.*action=\"([^\"]+)\".*>");

  private static final Pattern HIDDEN_CODE_PATTERN =
      Pattern.compile("<input type=\"hidden\" name=\"code\" value=\"([^\"]+)\">");

  private final GrantType grantType;
  private final String username;
  private final String password;
  private final ExecutorService executor;

  private final PipedOutputStream pipeOut = new PipedOutputStream();
  private final PipedInputStream pipeIn = new PipedInputStream(pipeOut);
  private final PrintStream consoleOut = new PrintStream(pipeOut);
  private final BufferedReader consoleIn = new BufferedReader(new InputStreamReader(pipeIn, UTF_8));
  private final PrintStream standardOut;

  private volatile boolean replaceSystemOut;
  private volatile boolean closing;
  private volatile Throwable error;
  private volatile URI baseUri;
  private volatile Consumer<URL> authUrlListener;
  private volatile Consumer<Throwable> errorListener;
  private volatile String authorizationCode;
  private volatile int expectedCallbackStatus = HTTP_OK;
  private volatile boolean denyConsent = false;

  public ResourceOwnerEmulator(GrantType grantType) throws IOException {
    this(grantType, null, null);
  }

  public ResourceOwnerEmulator(GrantType grantType, String username, String password)
      throws IOException {
    this.grantType = grantType;
    this.username = username;
    this.password = password;
    AtomicInteger counter = new AtomicInteger(1);
    executor =
        Executors.newFixedThreadPool(
            2, r -> new Thread(r, "ResourceOwnerEmulator-" + counter.getAndIncrement()));
    standardOut = System.out;
    executor.submit(this::readConsole);
  }

  public PrintStream getConsoleOut() {
    return consoleOut;
  }

  public void setAuthServerBaseUri(URI baseUri) {
    this.baseUri = baseUri;
  }

  public void replaceSystemOut() {
    this.replaceSystemOut = true;
    System.setOut(consoleOut);
  }

  public void overrideAuthorizationCode(String code, Status expectedStatus) {
    authorizationCode = code;
    expectedCallbackStatus = expectedStatus.getCode();
  }

  public void denyConsent() {
    this.denyConsent = true;
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
      URL authUrl = null;
      while ((line = consoleIn.readLine()) != null) {
        standardOut.println(line);
        if (line.startsWith(AuthorizationCodeFlow.MSG_PREFIX) && line.contains("http")) {
          authUrl = new URL(line.substring(line.indexOf("http")));
          Consumer<URL> listener = authUrlListener;
          if (listener != null) {
            listener.accept(authUrl);
          }
          if (grantType == GrantType.AUTHORIZATION_CODE) {
            URL finalAuthUrl = authUrl;
            executor.submit(() -> triggerAuthorizationCodeFlow(finalAuthUrl));
          }
        }
        if (line.matches(Pattern.quote(DeviceCodeFlow.MSG_PREFIX) + "[A-Z]{4}-[A-Z]{4}")) {
          URL finalAuthUrl = authUrl;
          assertThat(finalAuthUrl).isNotNull();
          String userCode = line.substring(DeviceCodeFlow.MSG_PREFIX.length());
          executor.submit(() -> triggerDeviceCodeFlow(finalAuthUrl, userCode));
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
  private void triggerAuthorizationCodeFlow(URL initialUrl) {
    try {
      LOGGER.info("Starting authorization code flow.");
      Set<String> cookies = new HashSet<>();
      URL callbackUrl =
          username == null || password == null
              ? readRedirectUrl((HttpURLConnection) initialUrl.openConnection(), cookies)
              : login(initialUrl, cookies);
      invokeCallbackUrl(callbackUrl);
      LOGGER.info("Authorization code flow completed.");
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /**
   * Emulate user browsing to the authorization URL printed on the console, then following the
   * instructions, entering the user code and optionally logging in with their credentials.
   */
  private void triggerDeviceCodeFlow(URL initialUrl, String userCode) {
    try {
      LOGGER.info("Starting device code flow.");
      Set<String> cookies = new HashSet<>();
      URL loginPageUrl = enterUserCode(initialUrl, userCode, cookies);
      if (username != null && password != null) {
        assertThat(loginPageUrl).isNotNull();
        URL consentPageUrl = login(loginPageUrl, cookies);
        authorizeDevice(consentPageUrl, cookies);
      }
      LOGGER.info("Device code flow completed.");
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /** Emulate user logging in to the authorization server. */
  private URL login(URL loginPageUrl, Set<String> cookies) throws IOException {
    // receive login page
    HttpURLConnection loginPageConn = openConnection(loginPageUrl);
    loginPageConn.setRequestMethod("GET");
    writeCookies(loginPageConn, cookies);
    String loginHtml = readHtml(loginPageConn);
    assertThat(loginPageConn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(loginPageConn, cookies);
    Matcher matcher = FORM_ACTION_PATTERN.matcher(loginHtml);
    assertThat(matcher.find()).isTrue();
    URL loginActionUrl = new URL(matcher.group(1));
    // send login form
    HttpURLConnection loginActionConn = openConnection(loginActionUrl);
    loginActionConn.setRequestMethod("POST");
    loginActionConn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    writeCookies(loginActionConn, cookies);
    loginActionConn.setDoOutput(true);
    String data =
        "username="
            + URLEncoder.encode(username, "UTF-8")
            + "&"
            + "password="
            + URLEncoder.encode(password, "UTF-8")
            + "&credentialId=";
    loginActionConn.getOutputStream().write(data.getBytes(UTF_8));
    loginActionConn.getOutputStream().close();
    return readRedirectUrl(loginActionConn, cookies);
  }

  /** Emulate browser being redirected to callback URL. */
  private void invokeCallbackUrl(URL callbackUrl) throws IOException {
    assertThat(callbackUrl)
        .hasPath(AuthorizationCodeFlow.CONTEXT_PATH)
        .hasParameter("code")
        .hasParameter("state");
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

  /** Emulate user entering provided user code on the authorization server. */
  private URL enterUserCode(URL codePageUrl, String userCode, Set<String> cookies)
      throws IOException {
    // receive device code page
    HttpURLConnection codeUrlConn = openConnection(codePageUrl);
    codeUrlConn.setRequestMethod("GET");
    writeCookies(codeUrlConn, cookies);
    assertThat(codeUrlConn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(codeUrlConn, cookies);
    // send device code form to same URL but with POST
    HttpURLConnection codeActionConn = openConnection(codePageUrl);
    codeActionConn.setRequestMethod("POST");
    codeActionConn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    codeActionConn.setDoOutput(true);
    String data = "device_user_code=" + URLEncoder.encode(userCode, "UTF-8");
    codeActionConn.getOutputStream().write(data.getBytes(UTF_8));
    codeActionConn.getOutputStream().close();
    if (username != null && password != null) {
      return readRedirectUrl(codeActionConn, cookies);
    } else {
      assertThat(codeActionConn.getResponseCode()).isEqualTo(HTTP_OK);
      return null;
    }
  }

  /** Emulate user consenting to authorize device on the authorization server. */
  private void authorizeDevice(URL consentPageUrl, Set<String> cookies) throws IOException {
    // receive consent page
    HttpURLConnection consentPageConn = openConnection(consentPageUrl);
    consentPageConn.setRequestMethod("GET");
    writeCookies(consentPageConn, cookies);
    String consentHtml = readHtml(consentPageConn);
    assertThat(consentPageConn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(consentPageConn, cookies);
    Matcher matcher = FORM_ACTION_PATTERN.matcher(consentHtml);
    assertThat(matcher.find()).isTrue();
    String formAction = matcher.group(1);
    matcher = HIDDEN_CODE_PATTERN.matcher(consentHtml);
    assertThat(matcher.find()).isTrue();
    String deviceCode = matcher.group(1);
    // send consent form
    URL consentActionUrl =
        new URL(
            consentPageUrl.getProtocol(),
            consentPageUrl.getHost(),
            consentPageUrl.getPort(),
            formAction);
    HttpURLConnection consentActionConn = openConnection(consentActionUrl);
    consentActionConn.setRequestMethod("POST");
    consentActionConn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    writeCookies(consentActionConn, cookies);
    consentActionConn.setDoOutput(true);
    String data = "code=" + URLEncoder.encode(deviceCode, "UTF-8");
    if (denyConsent) {
      data += "&cancel=No";
    } else {
      data += "&accept=Yes";
    }
    consentActionConn.getOutputStream().write(data.getBytes(UTF_8));
    consentActionConn.getOutputStream().close();
    assertThat(consentActionConn.getResponseCode()).isEqualTo(HTTP_OK);
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

  /**
   * Open a connection to the given URL, optionally replacing hostname and port with those actually
   * accessible by the client; this is necessary because the auth server may be sending URLs with
   * its configured frontend hostname + port, which, usually when using Docker, is something like
   * keycloak:8080.
   *
   * <p>FIXME: unfortunately this is not enough when KC_HOSTNAME_URL is set to keycloak:8080 and the
   * device flow is being used: the consent URL returns 404 when localhost is used instead :-(
   */
  private HttpURLConnection openConnection(URL url) throws IOException {
    HttpURLConnection conn;
    if (baseUri == null || baseUri.getHost().equals(url.getHost())) {
      conn = (HttpURLConnection) url.openConnection();
    } else {
      URL transformed =
          new URL(url.getProtocol(), baseUri.getHost(), baseUri.getPort(), url.getFile());
      conn = (HttpURLConnection) transformed.openConnection();
      conn.setRequestProperty("Host", url.getHost() + ":" + url.getPort());
    }
    return conn;
  }

  @Override
  public void close() throws Exception {
    closing = true;
    if (replaceSystemOut) {
      System.setOut(standardOut);
    }
    executor.shutdownNow();
    consoleIn.close();
    consoleOut.close();
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

  private static String readHtml(HttpURLConnection conn) throws IOException {
    String html;
    try (InputStream is = conn.getInputStream()) {
      html =
          new BufferedReader(new InputStreamReader(is, UTF_8))
              .lines()
              .collect(Collectors.joining("\n"));
    }
    return html;
  }

  private static URL readRedirectUrl(HttpURLConnection conn, Set<String> cookies)
      throws IOException {
    conn.setInstanceFollowRedirects(false);
    assertThat(conn.getResponseCode()).isIn(HTTP_MOVED_TEMP, HTTP_MOVED_PERM);
    String location = conn.getHeaderField("Location");
    assertThat(location).isNotNull();
    readCookies(conn, cookies);
    return new URL(location);
  }

  private static void readCookies(HttpURLConnection conn, Set<String> cookies) {
    List<String> cks = conn.getHeaderFields().get("Set-Cookie");
    if (cks != null) {
      cookies.addAll(cks);
    }
  }

  private static void writeCookies(HttpURLConnection conn, Set<String> cookies) {
    for (String c : cookies) {
      conn.addRequestProperty("Cookie", c);
    }
  }
}

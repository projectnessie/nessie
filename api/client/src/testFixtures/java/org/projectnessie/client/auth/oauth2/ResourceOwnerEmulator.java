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

import com.google.common.collect.ImmutableMap;
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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

  /**
   * Dummy factory method to circumvent class loading issues when instantiating this class from
   * within a Quarkus test.
   */
  public static ResourceOwnerEmulator forDeviceCode() throws IOException {
    return new ResourceOwnerEmulator(GrantType.DEVICE_CODE);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceOwnerEmulator.class);

  private static final Pattern FORM_ACTION_PATTERN =
      Pattern.compile("<form.*action=\"([^\"]+)\".*>");

  private static final Pattern HIDDEN_CODE_PATTERN =
      Pattern.compile("<input type=\"hidden\" name=\"code\" value=\"([^\"]+)\">");

  private static final Pattern USER_CODE_PATTERN =
      Pattern.compile(Pattern.quote(DeviceCodeFlow.MSG_PREFIX) + "[A-Z]{4}-[A-Z]{4}");

  private static final AtomicInteger COUNTER = new AtomicInteger(1);

  private final GrantType grantType;
  private final String username;
  private final String password;
  private final ExecutorService executor;

  private final PipedOutputStream pipeOut = new PipedOutputStream();
  private final PipedInputStream pipeIn = new PipedInputStream(pipeOut);
  private final PrintStream consoleOut = new PrintStream(pipeOut, true, "UTF-8");
  private final BufferedReader consoleIn = new BufferedReader(new InputStreamReader(pipeIn, UTF_8));
  private final PrintStream standardOut;

  private URL authUrl;
  private String userCode;

  private volatile boolean replaceSystemOut;
  private volatile boolean closing;
  private volatile Throwable error;
  private volatile URI baseUri;
  private volatile Consumer<URL> authUrlListener;
  private volatile Consumer<Throwable> errorListener;
  private volatile Runnable flowCompletionListener;
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
    executor =
        Executors.newFixedThreadPool(
            2, r -> new Thread(r, "ResourceOwnerEmulator-" + COUNTER.getAndIncrement()));
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

  public void setFlowCompletionListener(Runnable listener) {
    this.flowCompletionListener = listener;
  }

  private void readConsole() {
    try {
      String line;
      while ((line = consoleIn.readLine()) != null) {
        standardOut.println(line);
        standardOut.flush();
        switch (grantType) {
          case AUTHORIZATION_CODE:
            if (line.startsWith(AuthorizationCodeFlow.MSG_PREFIX) && line.contains("http")) {
              authUrl = extractAuthUrl(line);
              executor.submit(this::triggerAuthorizationCodeFlow);
            }
            break;
          case DEVICE_CODE:
            if (line.startsWith(DeviceCodeFlow.MSG_PREFIX) && line.contains("http")) {
              authUrl = extractAuthUrl(line);
            }
            if (USER_CODE_PATTERN.matcher(line).matches()) {
              assertThat(authUrl).isNotNull();
              userCode = line.substring(DeviceCodeFlow.MSG_PREFIX.length());
              executor.submit(this::triggerDeviceCodeFlow);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported grant type: " + grantType);
        }
      }
    } catch (RuntimeException | AssertionError | IOException t) {
      recordFailure(t);
    }
  }

  /**
   * Emulate user browsing to the authorization URL printed on the console, then following the
   * instructions and optionally logging in with their credentials.
   */
  private void triggerAuthorizationCodeFlow() {
    try {
      LOGGER.info("Starting authorization code flow.");
      Set<String> cookies = new HashSet<>();
      URL callbackUrl;
      if (username == null || password == null) {
        HttpURLConnection conn = (HttpURLConnection) authUrl.openConnection();
        callbackUrl = readRedirectUrl(conn, cookies);
        conn.disconnect();
      } else {
        callbackUrl = login(authUrl, cookies);
      }
      invokeCallbackUrl(callbackUrl);
      LOGGER.info("Authorization code flow completed.");
      Runnable listener = flowCompletionListener;
      if (listener != null) {
        listener.run();
      }
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /**
   * Emulate user browsing to the authorization URL printed on the console, then following the
   * instructions, entering the user code and optionally logging in with their credentials.
   */
  private void triggerDeviceCodeFlow() {
    try {
      LOGGER.info("Starting device code flow.");
      Set<String> cookies = new HashSet<>();
      URL loginPageUrl = enterUserCode(authUrl, userCode, cookies);
      if (loginPageUrl != null) {
        URL consentPageUrl = login(loginPageUrl, cookies);
        authorizeDevice(consentPageUrl, cookies);
      }
      LOGGER.info("Device code flow completed.");
      Runnable listener = flowCompletionListener;
      if (listener != null) {
        listener.run();
      }
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  private URL extractAuthUrl(String line) {
    URL authUrl;
    try {
      authUrl = new URL(line.substring(line.indexOf("http")));
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    Consumer<URL> listener = authUrlListener;
    if (listener != null) {
      listener.accept(authUrl);
    }
    return authUrl;
  }

  /** Emulate user logging in to the authorization server. */
  private URL login(URL loginPageUrl, Set<String> cookies) throws IOException {
    LOGGER.info("Performing login...");
    // receive login page
    HttpURLConnection loginPageConn = openConnection(loginPageUrl);
    loginPageConn.setRequestMethod("GET");
    writeCookies(loginPageConn, cookies);
    String loginHtml = readHtml(loginPageConn);
    assertThat(loginPageConn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(loginPageConn, cookies);
    loginPageConn.disconnect();
    Matcher matcher = FORM_ACTION_PATTERN.matcher(loginHtml);
    assertThat(matcher.find()).isTrue();
    URL loginActionUrl = new URL(matcher.group(1));
    // send login form
    HttpURLConnection loginActionConn = openConnection(loginActionUrl);
    Map<String, String> data =
        ImmutableMap.of(
            "username", username,
            "password", password,
            "credentialId", "");
    postForm(loginActionConn, data, cookies);
    URL redirectUrl = readRedirectUrl(loginActionConn, cookies);
    loginActionConn.disconnect();
    return redirectUrl;
  }

  /** Emulate browser being redirected to callback URL. */
  private void invokeCallbackUrl(URL callbackUrl) throws IOException {
    LOGGER.info("Opening callback URL...");
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
    HttpURLConnection conn = (HttpURLConnection) callbackUrl.openConnection();
    conn.setRequestMethod("GET");
    int status = conn.getResponseCode();
    conn.disconnect();
    assertThat(status).isEqualTo(expectedCallbackStatus);
  }

  /** Emulate user entering provided user code on the authorization server. */
  private URL enterUserCode(URL codePageUrl, String userCode, Set<String> cookies)
      throws IOException {
    LOGGER.info("Entering user code...");
    // receive device code page
    HttpURLConnection codePageConn = openConnection(codePageUrl);
    codePageConn.setRequestMethod("GET");
    writeCookies(codePageConn, cookies);
    assertThat(codePageConn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(codePageConn, cookies);
    codePageConn.disconnect();
    // send device code form to same URL but with POST
    HttpURLConnection codeActionConn = openConnection(codePageUrl);
    Map<String, String> data = ImmutableMap.of("device_user_code", userCode);
    postForm(codeActionConn, data, cookies);
    URL loginUrl;
    if (username != null && password != null) {
      // Expect a redirect to the login page
      loginUrl = readRedirectUrl(codeActionConn, cookies);
    } else {
      // Unit tests: expect just a 200 OK
      assertThat(codeActionConn.getResponseCode()).isEqualTo(HTTP_OK);
      loginUrl = null;
    }
    codeActionConn.disconnect();
    return loginUrl;
  }

  /** Emulate user consenting to authorize device on the authorization server. */
  private void authorizeDevice(URL consentPageUrl, Set<String> cookies) throws IOException {
    LOGGER.info("Authorizing device...");
    // receive consent page
    HttpURLConnection consentPageConn = openConnection(consentPageUrl);
    consentPageConn.setRequestMethod("GET");
    writeCookies(consentPageConn, cookies);
    String consentHtml = readHtml(consentPageConn);
    assertThat(consentPageConn.getResponseCode()).isEqualTo(HTTP_OK);
    readCookies(consentPageConn, cookies);
    consentPageConn.disconnect();
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
    Map<String, String> data =
        denyConsent
            ? ImmutableMap.of("code", deviceCode, "cancel", "No")
            : ImmutableMap.of("code", deviceCode, "accept", "Yes");
    postForm(consentActionConn, data, cookies);
    // Read the response but discard it, as it points to a static success HTML page
    readRedirectUrl(consentActionConn, cookies);
    consentActionConn.disconnect();
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
   * accessible by this client; this is necessary because the auth server may be sending URLs with a
   * hostname + port address that is only accessible within a Docker network, e.g. keycloak:8080.
   */
  private HttpURLConnection openConnection(URL url) throws IOException {
    HttpURLConnection conn;
    if (baseUri == null || baseUri.getHost().equals(url.getHost())) {
      conn = (HttpURLConnection) url.openConnection();
    } else {
      URL transformed =
          new URL(baseUri.getScheme(), baseUri.getHost(), baseUri.getPort(), url.getFile());
      conn = (HttpURLConnection) transformed.openConnection();
    }
    // See https://github.com/projectnessie/nessie/issues/7918
    conn.addRequestProperty("Accept", "text/html, *; q=.2, */*; q=.2");
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

  private static void postForm(
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
    LOGGER.info("Redirected to: {}", location);
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

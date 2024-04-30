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
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeviceCodeResourceOwnerEmulator extends InteractiveResourceOwnerEmulator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DeviceCodeResourceOwnerEmulator.class);

  private static final Pattern FORM_ACTION_PATTERN =
      Pattern.compile("<form.*action=\"([^\"]+)\".*>");

  private static final Pattern HIDDEN_CODE_PATTERN =
      Pattern.compile("<input type=\"hidden\" name=\"code\" value=\"([^\"]+)\">");

  private static final Pattern USER_CODE_PATTERN =
      Pattern.compile(Pattern.quote(DeviceCodeFlow.MSG_PREFIX) + "[A-Z]{4}-[A-Z]{4}");

  private URL authUrl;
  private String userCode;

  private volatile boolean denyConsent = false;

  /** Creates a new emulator with implicit login (for unit tests). */
  public DeviceCodeResourceOwnerEmulator() throws IOException {
    super(null, null);
  }

  /**
   * Creates a new emulator with required user login using the given username and password (for
   * integration tests).
   */
  public DeviceCodeResourceOwnerEmulator(String username, String password) throws IOException {
    super(username, password);
  }

  public void denyConsent() {
    this.denyConsent = true;
  }

  @Override
  protected Runnable processLine(String line) {
    if (line.startsWith(DeviceCodeFlow.MSG_PREFIX) && line.contains("http")) {
      authUrl = extractAuthUrl(line);
    }
    if (USER_CODE_PATTERN.matcher(line).matches()) {
      assertThat(authUrl).isNotNull();
      userCode = line.substring(DeviceCodeFlow.MSG_PREFIX.length());
      return this::triggerDeviceCodeFlow;
    }
    return null;
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
      notifyFlowCompleted();
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /** Emulate user entering provided user code on the authorization server. */
  private URL enterUserCode(URL codePageUrl, String userCode, Set<String> cookies)
      throws IOException {
    LOGGER.info("Entering user code...");
    // receive device code page
    getHtmlPage(codePageUrl, cookies);
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
    String consentHtml = getHtmlPage(consentPageUrl, cookies);
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
}

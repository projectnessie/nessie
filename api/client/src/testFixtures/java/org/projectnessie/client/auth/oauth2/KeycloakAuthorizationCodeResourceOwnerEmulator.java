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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.http.impl.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeycloakAuthorizationCodeResourceOwnerEmulator
    extends AbstractKeycloakResourceOwnerEmulator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KeycloakAuthorizationCodeResourceOwnerEmulator.class);

  private URI authUrl;

  private volatile String authorizationCode;
  private volatile int expectedCallbackStatus = HTTP_OK;

  /** Creates a new emulator with implicit login (for unit tests). */
  public KeycloakAuthorizationCodeResourceOwnerEmulator() throws IOException {
    super(null, null);
  }

  /**
   * Creates a new emulator with required user login using the given username and password (for
   * integration tests).
   */
  public KeycloakAuthorizationCodeResourceOwnerEmulator(String username, String password)
      throws IOException {
    super(username, password);
  }

  public void overrideAuthorizationCode(String code, Status expectedStatus) {
    authorizationCode = code;
    expectedCallbackStatus = expectedStatus.getCode();
  }

  @Override
  protected Runnable processLine(String line) {
    if (line.startsWith(AuthorizationCodeFlow.MSG_PREFIX) && line.contains("http")) {
      authUrl = extractAuthUrl(line);
      return this::triggerAuthorizationCodeFlow;
    }
    return null;
  }

  /**
   * Emulate user browsing to the authorization URL printed on the console, then following the
   * instructions and optionally logging in with their credentials.
   */
  private void triggerAuthorizationCodeFlow() {
    try {
      LOGGER.info("Starting authorization code flow.");
      Set<String> cookies = new HashSet<>();
      URI callbackUri;
      if (username == null || password == null) {
        HttpURLConnection conn = (HttpURLConnection) authUrl.toURL().openConnection();
        callbackUri = readRedirectUrl(conn, cookies);
        conn.disconnect();
      } else {
        callbackUri = login(authUrl, cookies);
      }
      invokeCallbackUrl(callbackUri);
      LOGGER.info("Authorization code flow completed.");
      notifyFlowCompleted();
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  /** Emulate browser being redirected to callback URL. */
  private void invokeCallbackUrl(URI callbackUrl) throws Exception {
    LOGGER.info("Opening callback URL...");
    assertThat(callbackUrl)
        .hasPath(AuthorizationCodeFlow.CONTEXT_PATH)
        .hasParameter("code")
        .hasParameter("state");
    String code = authorizationCode;
    if (code != null) {
      Map<String, String> params = HttpUtils.parseQueryString(callbackUrl.getQuery());
      callbackUrl =
          new URI(
              callbackUrl.getScheme(),
              null,
              callbackUrl.getHost(),
              callbackUrl.getPort(),
              callbackUrl.getPath(),
              "code=" + URLEncoder.encode(code, "UTF-8") + "&state=" + params.get("state"),
              null);
    }
    HttpURLConnection conn = (HttpURLConnection) callbackUrl.toURL().openConnection();
    conn.setRequestMethod("GET");
    int status = conn.getResponseCode();
    conn.disconnect();
    assertThat(status).isEqualTo(expectedCallbackStatus);
  }
}

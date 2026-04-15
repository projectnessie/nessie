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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.projectnessie.client.http.impl.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutheliaAuthorizationCodeResourceOwnerEmulator
    extends InteractiveResourceOwnerEmulator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AutheliaAuthorizationCodeResourceOwnerEmulator.class);

  private static final Pattern REDIRECT_PATTERN =
      Pattern.compile("\"redirect(?:_uri)?\":\"([^\"]+)\"");

  private URI authUrl;

  public AutheliaAuthorizationCodeResourceOwnerEmulator(
      String username, String password, SSLContext sslContext) throws IOException {
    super(username, password, sslContext);
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
      URI callbackUri = login(authUrl, cookies);
      invokeCallbackUrl(callbackUri);
      LOGGER.info("Authorization code flow completed.");
      notifyFlowCompleted();
    } catch (Exception | AssertionError t) {
      recordFailure(t);
    }
  }

  @Override
  protected URI login(URI loginPageUrl, Set<String> cookies) throws Exception {
    LOGGER.info("Performing login...");
    loginPageUrl = readRedirectUrl(openConnection(loginPageUrl), cookies);
    String loginHtml = getHtmlPage(loginPageUrl, cookies);
    assertThat(loginHtml).contains("data-basepath");
    Map<String, String> params = HttpUtils.parseQueryString(loginPageUrl.getRawQuery());
    String workflowId = params.get("workflow_id");
    URI loginActionUrl = loginPageUrl.resolve("/api/firstfactor");
    HttpURLConnection loginActionConn =
        openConnection(loginActionUrl, "application/json, text/plain, */*");
    String data =
        String.format(
            "{\"username\":\"%s\","
                + "\"password\":\"%s\","
                + "\"targetURL\":\"%s\","
                + "\"keepMeLoggedIn\":false,"
                + "\"workflow\":\"openid_connect\","
                + "\"workflowID\":\"%s"
                + "\"}",
            username, password, authUrl.toString(), workflowId);
    postJson(loginActionConn, data, cookies);
    int responseCode = loginActionConn.getResponseCode();
    assertThat(responseCode).isEqualTo(HTTP_OK);
    readCookies(loginActionConn, cookies);
    String response = readBody(loginActionConn);
    loginActionConn.disconnect();
    assertThat(response).contains("\"status\":\"OK\"");
    Matcher matcher = REDIRECT_PATTERN.matcher(response);
    assertThat(matcher.find()).isTrue();
    String redirectUri = matcher.group(1).replace("\\u0026", "&");
    HttpURLConnection redirectConn = openConnection(URI.create(redirectUri));
    writeCookies(redirectConn, cookies);
    URI nextUri = readRedirectUrl(redirectConn, cookies);
    if (nextUri.getPath().startsWith("/consent/")) {
      nextUri = acceptConsent(nextUri, cookies);
    }
    return nextUri;
  }

  /**
   * Authelia adds a consent decision step even with consent_mode: 'implicit'. The React SPA would
   * auto-submit consent via JavaScript in this case; we call the API directly instead.
   */
  private URI acceptConsent(URI consentPageUri, Set<String> cookies) throws Exception {
    LOGGER.info("Accepting consent at {}...", consentPageUri);
    Map<String, String> params = HttpUtils.parseQueryString(consentPageUri.getRawQuery());
    String flowId = params.get("flow_id");
    assertThat(flowId).as("flow_id must be present in consent URL").isNotNull();
    // POST consent acceptance to the Authelia consent API.
    // See ConsentPostRequestBody in authelia/internal/oidc/types.go.
    URI consentApiUrl = consentPageUri.resolve("/api/oidc/consent");
    HttpURLConnection consentConn =
        openConnection(consentApiUrl, "application/json, text/plain, */*");
    String data =
        String.format(
            "{\"flow_id\":\"%s\",\"client_id\":\"nessie-private-ac\",\"consent\":true,\"pre_configure\":false}",
            flowId);
    postJson(consentConn, data, cookies);
    int responseCode = consentConn.getResponseCode();
    assertThat(responseCode).isEqualTo(HTTP_OK);
    readCookies(consentConn, cookies);
    String consentResponse = readBody(consentConn);
    consentConn.disconnect();
    Matcher redirectMatcher = REDIRECT_PATTERN.matcher(consentResponse);
    assertThat(redirectMatcher.find())
        .as("Expected redirect in consent response: %s", consentResponse)
        .isTrue();
    String authzUri = redirectMatcher.group(1).replace("\\u0026", "&");
    HttpURLConnection authzConn = openConnection(URI.create(authzUri));
    writeCookies(authzConn, cookies);
    return readRedirectUrl(authzConn, cookies);
  }

  /** Emulate browser being redirected to callback URL. */
  private void invokeCallbackUrl(URI callbackUrl) throws Exception {
    LOGGER.info("Opening callback URL...");
    assertThat(callbackUrl)
        .hasPath(AuthorizationCodeFlow.CONTEXT_PATH)
        .hasParameter("code")
        .hasParameter("state");
    HttpURLConnection conn = (HttpURLConnection) callbackUrl.toURL().openConnection();
    conn.setRequestMethod("GET");
    int status = conn.getResponseCode();
    conn.disconnect();
    assertThat(status).isEqualTo(HTTP_OK);
  }
}

/*
 * Copyright (C) 2024 Dremio
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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKeycloakResourceOwnerEmulator
    extends InteractiveResourceOwnerEmulator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractKeycloakResourceOwnerEmulator.class);

  private static final Pattern LOGIN_FORM_ACTION_PATTERN =
      Pattern.compile("<form.*action=\"([^\"]+)\".*>");

  public AbstractKeycloakResourceOwnerEmulator(String username, String password)
      throws IOException {
    super(username, password);
  }

  @Override
  protected URI login(URI loginPageUrl, Set<String> cookies) throws Exception {
    LOGGER.info("Performing login...");
    // receive login page
    String loginHtml = getHtmlPage(loginPageUrl, cookies);
    Matcher matcher = LOGIN_FORM_ACTION_PATTERN.matcher(loginHtml);
    assertThat(matcher.find()).isTrue();
    URI loginActionUrl = new URI(matcher.group(1));
    // send login form
    HttpURLConnection loginActionConn = openConnection(loginActionUrl);
    Map<String, String> data =
        ImmutableMap.of(
            "username", username,
            "password", password,
            "credentialId", "");
    postForm(loginActionConn, data, cookies);
    URI redirectUrl = readRedirectUrl(loginActionConn, cookies);
    loginActionConn.disconnect();
    return redirectUrl;
  }
}

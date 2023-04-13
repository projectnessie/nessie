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

import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import org.projectnessie.client.auth.NessieAuthenticationProvider;
import org.projectnessie.client.http.HttpAuthentication;
import org.projectnessie.client.http.HttpClient;

public class OAuth2AuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "OAUTH2";

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public HttpAuthentication build(Function<String, String> configSupplier) {
    OAuth2ClientParams params = OAuth2ClientParams.fromConfig(configSupplier);
    OAuth2Client client = new OAuth2Client(params);
    client.start();
    return create(client);
  }

  public static HttpAuthentication create(OAuth2Authenticator authenticator) {
    return new OAuth2Authentication(authenticator);
  }

  private static class OAuth2Authentication implements HttpAuthentication {

    private final OAuth2Authenticator authenticator;

    private OAuth2Authentication(OAuth2Authenticator authenticator) {
      Objects.requireNonNull(
          authenticator,
          "OAuth2Authenticator must not be null for authentication type " + AUTH_TYPE_VALUE);
      this.authenticator = authenticator;
    }

    @Override
    public void applyToHttpClient(HttpClient.Builder client) {
      client.addRequestFilter(
          ctx -> {
            AccessToken token = authenticator.authenticate();
            ctx.putHeader(
                "Authorization", capitalize(token.getTokenType()) + " " + token.getPayload());
          });
    }

    @Override
    public void close() {
      authenticator.close();
    }
  }

  static String capitalize(String tokenType) {
    if (tokenType == null || tokenType.isEmpty()) {
      return "";
    }
    if (tokenType.length() == 1) {
      return tokenType.toUpperCase(Locale.ROOT);
    }
    return tokenType.substring(0, 1).toUpperCase(Locale.ROOT)
        + tokenType.substring(1).toLowerCase(Locale.ROOT);
  }
}

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
import org.projectnessie.client.http.RequestContext;

public class OAuth2AuthenticationProvider implements NessieAuthenticationProvider {

  public static final String AUTH_TYPE_VALUE = "OAUTH2";

  @Override
  public String getAuthTypeValue() {
    return AUTH_TYPE_VALUE;
  }

  @Override
  public HttpAuthentication build(Function<String, String> configSupplier) {
    return create(newAuthenticator(configSupplier));
  }

  /**
   * Creates a new {@link OAuth2Authenticator} from the given config supplier.
   *
   * <p>The returned authenticator is not started yet.
   */
  public static OAuth2Authenticator newAuthenticator(Function<String, String> configSupplier) {
    return newAuthenticator(OAuth2AuthenticatorConfig.fromConfigSupplier(configSupplier));
  }

  /**
   * Creates a new {@link OAuth2Authenticator} from the given config.
   *
   * <p>The returned authenticator is not started yet.
   */
  public static OAuth2Authenticator newAuthenticator(OAuth2AuthenticatorConfig config) {
    OAuth2ClientConfig clientConfig =
        config instanceof OAuth2ClientConfig
            ? (OAuth2ClientConfig) config
            : OAuth2ClientConfig.builder().from(config).build();
    return new OAuth2Client(clientConfig);
  }

  /**
   * Creates a new {@link OAuth2Authenticator} from the given config.
   *
   * <p>The returned authentication is not started yet.
   */
  public static HttpAuthentication create(OAuth2AuthenticatorConfig config) {
    return create(newAuthenticator(config));
  }

  /**
   * Creates a new {@link OAuth2Authenticator} from the given authenticator.
   *
   * <p>The returned authentication is not started yet.
   */
  public static HttpAuthentication create(OAuth2Authenticator authenticator) {
    return new OAuth2Authentication(authenticator);
  }

  static class OAuth2Authentication implements HttpAuthentication {

    private final OAuth2Authenticator authenticator;

    OAuth2Authentication(OAuth2Authenticator authenticator) {
      Objects.requireNonNull(
          authenticator,
          "OAuth2Authenticator must not be null for authentication type " + AUTH_TYPE_VALUE);
      this.authenticator = authenticator;
    }

    @Override
    public void start() {
      authenticator.start();
    }

    @Override
    public HttpAuthentication copy() {
      return new OAuth2Authentication(authenticator.copy());
    }

    @Override
    public void applyToHttpClient(HttpClient.Builder client) {
      client.addRequestFilter(this::applyToHttpRequest);
    }

    @Override
    public void applyToHttpRequest(RequestContext context) {
      AccessToken token = authenticator.authenticate();
      if (!token.getTokenType().toLowerCase(Locale.ROOT).equals("bearer")) {
        throw new IllegalArgumentException(
            "OAuth2 token type returned from the authenticating server must be 'Bearer', but was: "
                + token.getTokenType());
      }
      context.putHeader("Authorization", "Bearer " + token.getPayload());
    }

    @Override
    public void close() {
      authenticator.close();
    }
  }
}

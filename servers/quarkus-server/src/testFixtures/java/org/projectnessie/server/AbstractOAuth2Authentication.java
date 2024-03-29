/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.NessieAuthentication;
import org.projectnessie.client.auth.oauth2.GrantType;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;
import org.projectnessie.client.auth.oauth2.OAuth2Exception;
import org.projectnessie.client.auth.oauth2.ResourceOwnerEmulator;
import org.projectnessie.client.http.Status;

@SuppressWarnings("resource") // api() returns an AutoCloseable
public abstract class AbstractOAuth2Authentication extends BaseClientAuthTest {

  @Test
  void testAuthorizedClientCredentials() throws Exception {
    NessieAuthentication authentication = oauth2Authentication(clientCredentialsConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThat(api().getAllReferences().stream()).isNotEmpty();
  }

  @Test
  void testAuthorizedPassword() throws Exception {
    NessieAuthentication authentication = oauth2Authentication(passwordConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThat(api().getAllReferences().stream()).isNotEmpty();
  }

  @Test
  void testAuthorizedAuthorizationCode() throws Exception {
    try (ResourceOwnerEmulator ignored = newResourceOwner(GrantType.AUTHORIZATION_CODE)) {
      NessieAuthentication authentication = oauth2Authentication(authorizationCodeConfig());
      withClientCustomizer(b -> b.withAuthentication(authentication));
      assertThat(api().getAllReferences().stream()).isNotEmpty();
    }
  }

  @Test
  void testAuthorizedDeviceCode() throws Exception {
    try (ResourceOwnerEmulator ignored = newResourceOwner(GrantType.DEVICE_CODE)) {
      NessieAuthentication authentication = oauth2Authentication(deviceCodeConfig());
      withClientCustomizer(b -> b.withAuthentication(authentication));
      assertThat(api().getAllReferences().stream()).isNotEmpty();
    }
  }

  protected abstract ResourceOwnerEmulator newResourceOwner(GrantType grantType) throws IOException;

  /**
   * This test expects the OAuthClient to fail with a 401 UNAUTHORIZED, not Nessie. It is too
   * difficult to configure Keycloak to return a 401 UNAUTHORIZED for a token that was successfully
   * obtained with the OAuthClient.
   */
  @Test
  void testUnauthorized() {
    NessieAuthentication authentication = oauth2Authentication(wrongPasswordConfig());
    withClientCustomizer(b -> b.withAuthentication(authentication));
    assertThatThrownBy(() -> api().getAllReferences().stream())
        .asInstanceOf(type(OAuth2Exception.class))
        .extracting(OAuth2Exception::getStatus)
        .isEqualTo(Status.UNAUTHORIZED);
  }

  protected Properties clientCredentialsConfig() {
    Properties config = new Properties();
    config.setProperty("nessie.authentication.oauth2.grant-type", "client_credentials");
    config.setProperty("nessie.authentication.oauth2.client-id", "quarkus-service-app");
    config.setProperty("nessie.authentication.oauth2.client-secret", "secret");
    config.setProperty("nessie.authentication.oauth2.client-scopes", "scope1 scope2");
    return config;
  }

  protected Properties passwordConfig() {
    Properties config = clientCredentialsConfig();
    config.setProperty("nessie.authentication.oauth2.grant-type", "password");
    config.setProperty("nessie.authentication.oauth2.username", "alice");
    config.setProperty("nessie.authentication.oauth2.password", "alice");
    return config;
  }

  protected Properties authorizationCodeConfig() {
    Properties config = clientCredentialsConfig();
    config.setProperty("nessie.authentication.oauth2.grant-type", "authorization_code");
    config.setProperty("nessie.authentication.oauth2.auth-code-flow.web-port", "8989");
    return config;
  }

  protected Properties deviceCodeConfig() {
    Properties config = clientCredentialsConfig();
    config.setProperty("nessie.authentication.oauth2.grant-type", "device_code");
    return config;
  }

  protected Properties wrongPasswordConfig() {
    Properties config = passwordConfig();
    config.setProperty("nessie.authentication.oauth2.password", "WRONG");
    return config;
  }

  protected NessieAuthentication oauth2Authentication(Properties config) {
    return new OAuth2AuthenticationProvider().build(config::getProperty);
  }
}

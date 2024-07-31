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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.keycloak.client.KeycloakTestClient;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import org.projectnessie.client.auth.oauth2.GrantType;
import org.projectnessie.client.auth.oauth2.KeycloakAuthorizationCodeResourceOwnerEmulator;
import org.projectnessie.client.auth.oauth2.KeycloakDeviceCodeResourceOwnerEmulator;
import org.projectnessie.client.auth.oauth2.ResourceOwnerEmulator;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@QuarkusIntegrationTest
@WithTestResource(
    value = KeycloakTestResourceLifecycleManager.class,
    initArgs =
        @ResourceArg(
            name = KeycloakTestResourceLifecycleManager.KEYCLOAK_CLIENT_SCOPES,
            value = "scope1,scope2"))
@TestProfile(ITOAuth2Authentication.Profile.class)
public class ITOAuth2Authentication extends AbstractOAuth2Authentication {

  private final KeycloakTestClient keycloakClient = new KeycloakTestClient();

  @Override
  protected Properties clientCredentialsConfig() {
    Properties config = super.clientCredentialsConfig();
    String issuerUrl = keycloakClient.getAuthServerUrl();
    config.setProperty("nessie.authentication.oauth2.issuer-url", issuerUrl);
    return config;
  }

  @Override
  protected Properties deviceCodeConfig() {
    Properties config = super.deviceCodeConfig();
    // Keycloak advertises the token endpoint using whichever hostname was provided in the request,
    // but the authentication endpoints are always advertised at keycloak:8080, which is
    // the configured KC_HOSTNAME_URL env var and corresponds to the Docker internal network
    // address. This works for the authorization code flow because ResourceOwnerEmulator knows how
    // to deal with this; but for the device flow we need to use a different hostname, so endpoint
    // discovery is not an option here.
    config.setProperty(
        "nessie.authentication.oauth2.device-auth-endpoint",
        keycloakClient.getAuthServerUrl() + "/protocol/openid-connect/auth/device");
    return config;
  }

  @Override
  protected ResourceOwnerEmulator newResourceOwner(GrantType grantType) throws IOException {
    return switch (grantType) {
      case CLIENT_CREDENTIALS, PASSWORD -> ResourceOwnerEmulator.INACTIVE;
      case AUTHORIZATION_CODE -> newAuthorizationCodeResourceOwner();
      case DEVICE_CODE -> newDeviceCodeResourceOwner();
      default -> throw new IllegalArgumentException("Unsupported grant type: " + grantType);
    };
  }

  private KeycloakAuthorizationCodeResourceOwnerEmulator newAuthorizationCodeResourceOwner()
      throws IOException {
    KeycloakAuthorizationCodeResourceOwnerEmulator resourceOwner =
        new KeycloakAuthorizationCodeResourceOwnerEmulator("alice", "alice");
    resourceOwner.replaceSystemOut();
    resourceOwner.setAuthServerBaseUri(URI.create(keycloakClient.getAuthServerUrl()));
    resourceOwner.setErrorListener(e -> api().close());
    return resourceOwner;
  }

  private KeycloakDeviceCodeResourceOwnerEmulator newDeviceCodeResourceOwner() throws IOException {
    KeycloakDeviceCodeResourceOwnerEmulator resourceOwner =
        new KeycloakDeviceCodeResourceOwnerEmulator("alice", "alice");
    resourceOwner.replaceSystemOut();
    resourceOwner.setAuthServerBaseUri(URI.create(keycloakClient.getAuthServerUrl()));
    resourceOwner.setErrorListener(e -> api().close());
    return resourceOwner;
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(AuthenticationEnabledProfile.AUTH_CONFIG_OVERRIDES)
          .build();
    }
  }
}

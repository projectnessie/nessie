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
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.keycloak.client.KeycloakTestClient;
import java.io.IOException;
import java.util.Map;
import org.projectnessie.client.auth.oauth2.ResourceOwnerEmulator;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@QuarkusIntegrationTest
@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = KeycloakTestResourceLifecycleManager.class)
@TestProfile(ITOAuth2Authentication.Profile.class)
public class ITOAuth2Authentication extends AbstractOAuth2Authentication {

  private final KeycloakTestClient keycloakClient = new KeycloakTestClient();

  @Override
  protected String tokenEndpoint() {
    return keycloakClient.getAuthServerUrl() + "/protocol/openid-connect/token";
  }

  @Override
  protected String authEndpoint() {
    return keycloakClient.getAuthServerUrl() + "/protocol/openid-connect/auth";
  }

  @Override
  protected ResourceOwnerEmulator newResourceOwner() throws IOException {
    return new ResourceOwnerEmulator("alice", "alice");
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

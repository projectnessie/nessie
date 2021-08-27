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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import io.smallrye.jwt.build.Jwt;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.auth.BearerAuthenticationProvider;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@QuarkusTest
@QuarkusTestResource(OidcWiremockTestResource.class)
@TestProfile(value = TestOpenIdAuthentication.Profile.class)
public class TestOpenIdAuthentication extends BaseClientAuthTest {

  private String getJwtToken() {
    return Jwt.preferredUserName("test_user").issuer("https://server.example.com").sign();
  }

  @Test
  void testValidJwt() {
    withClientCustomizer(
        b -> b.withAuthentication(BearerAuthenticationProvider.create(getJwtToken())));
    assertThat(client().getTreeApi().getAllReferences()).isNotEmpty();
  }

  @Test
  void testInvalidToken() {
    withClientCustomizer(
        b -> b.withAuthentication(BearerAuthenticationProvider.create("invalid_token")));
    assertThatThrownBy(() -> client().getTreeApi().getAllReferences())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  @Test
  void testAbsentToken() {
    assertThatThrownBy(() -> client().getTreeApi().getAllReferences())
        .isInstanceOfSatisfying(
            NessieNotAuthorizedException.class,
            e -> assertThat(e.getError().getStatus()).isEqualTo(401));
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(AuthenticationEnabledProfile.CONFIG_OVERRIDES)
          .put("quarkus.oidc.client-id", getClass().getName())
          .put("quarkus.oidc.auth-server-url", "${keycloak.url}/realms/quarkus/")
          .put("smallrye.jwt.sign.key.location", "privateKey.jwk")
          .build();
    }
  }
}

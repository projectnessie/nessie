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
import com.google.common.collect.ImmutableSet;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.oidc.server.OidcWiremockTestResource;
import io.smallrye.jwt.build.Jwt;
import java.util.Map;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@QuarkusTest
@WithTestResource(OidcWiremockTestResource.class)
@TestProfile(value = TestBearerAuthentication.Profile.class)
public class TestBearerAuthentication extends AbstractBearerAuthentication {

  @Override
  protected String getValidJwtToken() {
    return Jwt.preferredUserName("alice")
        .groups(ImmutableSet.of("user"))
        .issuer("https://server.example.com")
        .sign();
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(AuthenticationEnabledProfile.AUTH_CONFIG_OVERRIDES)
          // keycloak.url defined by OidcWiremockTestResource
          .put("quarkus.oidc.auth-server-url", "${keycloak.url}/realms/quarkus/")
          .build();
    }
  }
}

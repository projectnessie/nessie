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
package org.projectnessie.server.catalog.auth;

import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.net.URLConnection;
import java.util.Map;
import java.util.Properties;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientId;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakClientSecret;
import org.projectnessie.quarkus.tests.profiles.KeycloakTestResourceLifecycleManager.KeycloakTokenEndpointUri;
import org.projectnessie.server.catalog.AbstractTrino;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;
import org.projectnessie.testing.keycloak.OAuthUtils;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@WithTestResource(MinioTestResourceLifecycleManager.class)
@WithTestResource(KeycloakTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
@TestProfile(ITOAuthTrinoS3.Profile.class)
public class ITOAuthTrinoS3 extends AbstractTrino {
  @KeycloakTokenEndpointUri static URI tokenEndpoint;
  @KeycloakClientId static String clientId;
  @KeycloakClientSecret static String clientSecret;

  @Override
  protected URLConnection tweakSetupConfigCall(URLConnection urlConnection) throws Exception {
    String token = OAuthUtils.fetchToken(tokenEndpoint, clientId, clientSecret);

    urlConnection.setRequestProperty("Authorization", "Bearer " + token);
    return super.tweakSetupConfigCall(urlConnection);
  }

  @Override
  protected void tweakTrinoProperties(Properties props) {
    super.tweakTrinoProperties(props);

    String token = OAuthUtils.fetchToken(tokenEndpoint, clientId, clientSecret);
    props.put("iceberg.rest-catalog.oauth2.token", token);
    // Credentials flow impossible with Trino, Trino lacks ability to configure an OAuth2 endpoint.
    // props.put("iceberg.rest-catalog.oauth2.credential", clientId + ':' + clientSecret);
    props.remove("iceberg.rest-catalog.oauth2.credential");
  }

  public static class Profile extends BaseProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("nessie.server.authentication.enabled", "true")
          .build();
    }
  }
}

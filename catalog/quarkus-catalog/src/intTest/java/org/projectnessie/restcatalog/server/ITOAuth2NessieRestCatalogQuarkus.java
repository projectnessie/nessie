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
package org.projectnessie.restcatalog.server;

import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TYPE;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT;

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;
import org.projectnessie.client.auth.oauth2.OAuth2AuthenticationProvider;

/**
 * In this test, the REST Catalog server will use a Nessie client configured with the OAUTH2
 * authentication provider. The Iceberg client and the Nessie client will thus use different access
 * tokens, all obtained from the same Keycloak server.
 */
@QuarkusIntegrationTest
@TestProfile(ITOAuth2NessieRestCatalogQuarkus.Profile.class)
public class ITOAuth2NessieRestCatalogQuarkus extends BaseNessieRestCatalogTests {

  public static class Profile extends BaseNessieRestCatalogTests.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> conf = new HashMap<>(super.getConfigOverrides());
      conf.put(
          "nessie.iceberg.nessie-client.\"" + CONF_NESSIE_AUTH_TYPE + "\"",
          OAuth2AuthenticationProvider.AUTH_TYPE_VALUE);
      conf.put(
          "nessie.iceberg.nessie-client.\"" + CONF_NESSIE_OAUTH2_TOKEN_ENDPOINT + "\"",
          "${quarkus.oidc.token-path}");
      conf.put(
          "nessie.iceberg.nessie-client.\"" + CONF_NESSIE_OAUTH2_CLIENT_ID + "\"",
          "${quarkus.oidc.client-id}");
      conf.put(
          "nessie.iceberg.nessie-client.\"" + CONF_NESSIE_OAUTH2_CLIENT_SECRET + "\"",
          "${quarkus.oidc.credentials.secret}");
      return conf;
    }
  }
}

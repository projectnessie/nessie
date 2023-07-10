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

import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.Map;

/**
 * In this test, the REST Catalog server will use a Nessie client configured with the PROPAGATE
 * authentication provider. The Iceberg client and the Nessie client will thus use the same access
 * token, obtained from the Keycloak server by the Iceberg client through the REST Catalog server's
 * /oauth/tokens endpoint.
 */
@QuarkusIntegrationTest
@TestProfile(ITPropagatingNessieRestCatalogQuarkus.Profile.class)
public class ITPropagatingNessieRestCatalogQuarkus extends BaseNessieRestCatalogTests {

  public static class Profile extends BaseNessieRestCatalogTests.Profile {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> conf = new HashMap<>(super.getConfigOverrides());
      conf.put("nessie.iceberg.nessie-client.\"" + CONF_NESSIE_AUTH_TYPE + "\"", "PROPAGATE");
      return conf;
    }
  }
}

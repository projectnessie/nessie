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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.quarkus.tests.profiles.QuarkusTestProfilePersistInmemory;

@QuarkusTest
@TestProfile(TestMetricsDisabled.Profile.class)
public class TestMetricsDisabled extends AbstractQuarkusRest {

  @Test
  void smokeTestMetricsDisabled() {
    int managementPort = Integer.getInteger("quarkus.management.port");
    URI managementBaseUri;
    try {
      managementBaseUri =
          new URI("http", null, clientUri.getHost(), managementPort, "/", null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    RestAssured.given()
        .when()
        .baseUri(managementBaseUri.toString())
        .basePath("/q/metrics")
        .accept("*/*")
        .get()
        .then()
        .statusCode(404)
        .extract()
        .asString();
  }

  public static class Profile extends QuarkusTestProfilePersistInmemory {

    @Override
    public Map<String, String> getConfigOverrides() {
      Map<String, String> config = new HashMap<>(super.getConfigOverrides());
      config.put("quarkus.micrometer.enabled", "false");
      return config;
    }
  }
}

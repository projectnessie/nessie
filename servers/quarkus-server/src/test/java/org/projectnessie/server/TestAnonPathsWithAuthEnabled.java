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
package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.server.QuarkusNessieClientResolver.getQuarkusBaseTestUri;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.server.authn.AuthenticationEnabledProfile;

@QuarkusTest
@TestProfile(value = TestAnonPathsWithAuthEnabled.Profile.class)
public class TestAnonPathsWithAuthEnabled {
  @ParameterizedTest
  @CsvSource(
      value = {
        "POST,/iceberg/v1/oauth/tokens,501",
        "GET,/nessie-openapi/openapi.yaml,200",
        "HEAD,/nessie-openapi/openapi.yaml,200",
        "GET,/nessie-openapi/openapi.json,200",
        "HEAD,/nessie-openapi/openapi.json,200",
        "GET,/favicon.ico,200",
        "HEAD,/favicon.ico,200",
        //
        // Nessie-API
        "GET,/api/v1/config,401",
        "GET,/api/v2/config,401",
        "GET,/iceberg/v1/config,401",
        //
        // Web-UI
        "GET,/manifest.json,401",
        "HEAD,/manifest.json,401",
        "GET,/index.html,401",
        "HEAD,/index.html,401",
        "GET,/,401",
        "HEAD,/,401",
        "GET,/tree,401",
        "HEAD,/tree,401",
        "GET,/tree/foo/bar,401",
        "HEAD,/tree/foo/bar,401",
        "GET,/content,401",
        "GET,/notfound,401",
        "GET,/commits,401"
      })
  public void httpStatusForPath(String method, String path, int expectedStatus) throws Exception {
    var urlConnection =
        (HttpURLConnection) getQuarkusBaseTestUri().resolve(path).toURL().openConnection();
    urlConnection.setRequestMethod(method);
    try (var in = urlConnection.getInputStream()) {
      if ("HEAD".equals(method)) {
        assertThat(in.readAllBytes()).isEmpty();
      } else {
        assertThat(in.readAllBytes()).isNotEmpty();
      }
    } catch (IOException e) {
      // no-op, really
    }
    assertThat(urlConnection.getResponseCode()).isEqualTo(expectedStatus);
  }

  public static class Profile extends AuthenticationEnabledProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          .put("quarkus.http.auth.basic", "true")
          // Need a dummy URL to satisfy the Quarkus OIDC extension.
          .put("quarkus.oidc.auth-server-url", "http://127.255.0.0:0/auth/realms/unset/")
          .put("nessie.catalog.default-warehouse", WAREHOUSE_NAME)
          .put("nessie.catalog.warehouses." + WAREHOUSE_NAME + ".location", "s3://foo/")
          .build();
    }
  }
}

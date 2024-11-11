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
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@QuarkusTest
@TestProfile(value = TestAnonPathsWithAuthDisabled.Profile.class)
public class TestAnonPathsWithAuthDisabled {
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
        "GET,/manifest.json,200",
        "HEAD,/manifest.json,200",
        "HEAD,/robots.txt,200",
        "GET,/robots.txt,200",
        //
        // usually protected, but authn is disabled
        "GET,/api/v1/config,200",
        "GET,/api/v2/config,200",
        "GET,/iceberg/v1/config,200",
        //
        // Web-UI
        "GET,/tree,200",
        "HEAD,/tree,200",
        "GET,/tree/foo/bar,200",
        "HEAD,/tree/foo/bar,200",
        "GET,/content,200",
        "GET,/notfound,200",
        "GET,/commits,200",
        "GET,/api/v3/blah,404",
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

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("nessie.catalog.default-warehouse", WAREHOUSE_NAME)
          .put("nessie.catalog.warehouses." + WAREHOUSE_NAME + ".location", "s3://foo/")
          .build();
    }
  }
}

/*
 * Copyright (C) 2022 Dremio
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

import io.restassured.RestAssured;
import java.net.URI;
import java.net.URISyntaxException;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;

public abstract class AbstractQuarkusRestWithMetrics extends AbstractQuarkusRest {
  // We need to extend the base class because all Nessie metrics are created lazily.
  // They will appear in the `/q/metrics` endpoint only when some REST actions are executed.

  // this test is executed after all tests from the base class

  protected String getMetrics() {
    int managementPort = Integer.getInteger("quarkus.management.port");
    URI managementBaseUri;
    try {
      managementBaseUri =
          new URI("http", null, clientUri.getHost(), managementPort, "/", null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return RestAssured.given()
        .when()
        .baseUri(managementBaseUri.toString())
        .basePath("/q/metrics")
        .accept("*/*")
        .get()
        .then()
        .statusCode(200)
        .extract()
        .asString();
  }

  @Test
  void smokeTestMetrics() {
    String body = getMetrics();
    // Do not query JVM metrics in tests, see
    // https://github.com/quarkusio/quarkus/issues/24210#issuecomment-1064833013
    soft.assertThat(body).contains("process_cpu_usage");
    soft.assertThat(body)
        // also assert that VersionStore metrics have the global tag application="Nessie"
        .containsPattern(
            "nessie_versionstore_request_seconds_max\\{.*application=\""
                + getApplicationLabel()
                + "\".*}");
    soft.assertThat(body).contains("nessie_versionstore_request_seconds_bucket");
    soft.assertThat(body).contains("nessie_versionstore_request_seconds_count");
    soft.assertThat(body).contains("nessie_versionstore_request_seconds_sum");
    soft.assertThat(body).contains("http_server_connections_seconds_max");
    soft.assertThat(body).contains("http_server_connections_seconds_active_count");
  }

  protected String getApplicationLabel() {
    return "Nessie";
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1)
  void smokeHttpApiV1Metrics() {
    String body = getMetrics();
    soft.assertThat(body).contains("/api/v1/diffs/{diff_params}");
    soft.assertThat(body).contains("/api/v1/trees/{referenceType}/{ref}");
    soft.assertThat(body).contains("/api/v1/trees/branch/{branchName}/commit");
    soft.assertThat(body).contains("/api/v1/trees/branch/{branchName}/transplant");
    soft.assertThat(body).contains("/api/v1/trees/branch/{branchName}/merge");
    soft.assertThat(body).contains("/api/v1/trees/branch/{ref}");
    soft.assertThat(body).contains("/api/v1/trees/tree/{ref}/entries");
    soft.assertThat(body).contains("/api/v1/namespaces/{ref}");
    soft.assertThat(body).contains("/api/v1/namespaces/namespace/{ref}/{name}");
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void smokeHttpApiV2Metrics() {
    String body = getMetrics();
    soft.assertThat(body).contains("/api/v2/config");
    soft.assertThat(body).contains("/api/v2/trees/{ref}");
    soft.assertThat(body).contains("/api/v2/trees/{ref}/contents");
    soft.assertThat(body).contains("/api/v2/trees/{ref}/entries");
    soft.assertThat(body).contains("/api/v2/trees/{ref}/history");
    soft.assertThat(body).contains("/api/v2/trees/{ref}/history/commit");
    soft.assertThat(body).contains("/api/v2/trees/{ref}/history/merge");
    soft.assertThat(body).contains("/api/v2/trees/{ref}/history/transplant");
    soft.assertThat(body).contains("/api/v2/trees/{from-ref}/diff/{to-ref}");
  }
}

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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.restassured.RestAssured;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;

public abstract class AbstractQuarkusRestWithMetrics extends AbstractQuarkusRest {
  // We need to extend the base class because all Nessie metrics are created lazily.
  // They will appear in the `/q/metrics` endpoint only when some REST actions are executed.

  // this test is executed after all tests from the base class

  private String getMetrics() {
    return RestAssured.given()
        .when()
        .baseUri(clientUri.resolve("/").toString())
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
    assertThat(body).contains("process_cpu_usage");
    assertThat(body)
        // also assert that VersionStore metrics have the global tag application="Nessie"
        .containsPattern("nessie_versionstore_request_seconds_max\\{.*application=\"Nessie\".*}");
    assertThat(body).contains("nessie_versionstore_request_seconds_bucket");
    assertThat(body).contains("nessie_versionstore_request_seconds_count");
    assertThat(body).contains("nessie_versionstore_request_seconds_sum");
    assertThat(body).contains("http_server_connections_seconds_max");
    assertThat(body).contains("http_server_connections_seconds_active_count");
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1)
  void smokeHttpApiV1Metrics() {
    String body = getMetrics();
    assertThat(body).contains("/api/v1/diffs/{diff_params}");
    assertThat(body).contains("/api/v1/trees/{referenceType}/{ref}");
    assertThat(body).contains("/api/v1/trees/branch/{branchName}/commit");
    assertThat(body).contains("/api/v1/trees/branch/{branchName}/transplant");
    assertThat(body).contains("/api/v1/trees/branch/{branchName}/merge");
    assertThat(body).contains("/api/v1/trees/branch/{ref}");
    assertThat(body).contains("/api/v1/trees/tree/{ref}/entries");
    assertThat(body).contains("/api/v1/namespaces/{ref}");
    assertThat(body).contains("/api/v1/namespaces/namespace/{ref}/{name}");
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2)
  void smokeHttpApiV2Metrics() {
    String body = getMetrics();
    assertThat(body).contains("/api/v2/config");
    assertThat(body).contains("/api/v2/trees/{ref}");
    assertThat(body).contains("/api/v2/trees/{ref}/contents");
    assertThat(body).contains("/api/v2/trees/{ref}/entries");
    assertThat(body).contains("/api/v2/trees/{ref}/history");
    assertThat(body).contains("/api/v2/trees/{ref}/history/commit");
    assertThat(body).contains("/api/v2/trees/{ref}/history/merge");
    assertThat(body).contains("/api/v2/trees/{ref}/history/transplant");
    assertThat(body).contains("/api/v2/trees/{from-ref}/diff/{to-ref}");
  }
}

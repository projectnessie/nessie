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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import org.junit.jupiter.api.Test;
import org.projectnessie.jaxrs.AbstractTestRest;
import org.projectnessie.server.profiles.QuarkusTestProfileInmemory;

@QuarkusTest
@TestProfile(value = QuarkusTestProfileInmemory.class)
public class ITMetrics extends AbstractTestRest {
  // We need to extend the AbstractTestRest because all Nessie metrics are created lazily.
  // They will appear in the `/q/metrics` endpoint only when some REST actions are executed.

  // this test is executed after all tests from the AbstractTestRest
  @Test
  void smokeTestMetrics() {
    // when
    String body =
        RestAssured.given()
            .when()
            .basePath("/q/metrics")
            .get()
            .then()
            .statusCode(200)
            .extract()
            .asString();

    // then
    assertThat(body).contains("jvm_threads_live_threads");
    assertThat(body).contains("jvm_memory_committed_bytes");
    assertThat(body).contains("nessie_versionstore_request_seconds_max");
    assertThat(body).contains("nessie_versionstore_request_seconds_bucket");
    assertThat(body).contains("nessie_versionstore_request_seconds_count");
    assertThat(body).contains("nessie_versionstore_request_seconds_sum");
    assertThat(body).contains("/api/v1/diffs/{diff_params}");
    assertThat(body).contains("/api/v1/trees/{referenceType}/{ref}");
    assertThat(body).contains("/api/v1/trees/branch/{ref}");
    assertThat(body).contains("/api/v1/trees/tree/{ref}/entries");
    assertThat(body).contains("http_server_connections_seconds_max");
    assertThat(body).contains("http_server_connections_seconds_active_count");
    assertThat(body).contains("jvm_gc_live_data_size_bytes");
  }
}

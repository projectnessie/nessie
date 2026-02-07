/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.server.catalog;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.server.catalog.IcebergCatalogTestCommon.WAREHOUSE_NAME;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.vertx.http.HttpServer;
import io.restassured.http.ContentType;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that the X-Forwarded-Prefix header is correctly handled when constructing URIs returned by
 * the Iceberg REST config endpoint.
 */
@QuarkusTest
@TestProfile(TestForwardedPrefix.Profile.class)
public class TestForwardedPrefix {

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          // Non-empty root path to exercise concatenation of forwarded prefix and root path
          .put("quarkus.http.root-path", "nessie-root")
          .put("quarkus.http.proxy.proxy-address-forwarding", "true")
          .put("quarkus.http.proxy.allow-x-forwarded", "true")
          .put("quarkus.http.proxy.enable-forwarded-prefix", "true")
          .put("nessie.catalog.default-warehouse", WAREHOUSE_NAME)
          .put("nessie.catalog.warehouses." + WAREHOUSE_NAME + ".location", "s3://foo/")
          .build();
    }
  }

  private static final String REGEX = "http://localhost:\\d+/nessie-proxy/nessie-root/(\\w+/)+";

  @Inject HttpServer httpServer;

  @ParameterizedTest
  @ValueSource(strings = {"nessie-proxy", "/nessie-proxy", "nessie-proxy/", "/nessie-proxy/"})
  public void icebergConfigWithXForwardedPrefix(String forwardedPrefix) {

    @SuppressWarnings({"unchecked", "UastIncorrectHttpHeaderInspection"})
    Map<String, Object> response =
        given()
            .when()
            .baseUri(String.format("http://localhost:%d/", httpServer.getPort()))
            .header("X-Forwarded-Prefix", forwardedPrefix)
            .get("/iceberg/v1/config")
            .then()
            .statusCode(200)
            .contentType(ContentType.JSON)
            .extract()
            .as(Map.class);

    @SuppressWarnings("unchecked")
    Map<String, String> overrides = (Map<String, String>) response.get("overrides");

    assertThat(overrides).isNotNull();
    assertThat(overrides.get("uri")).matches(REGEX);
    assertThat(overrides.get("nessie.core-base-uri")).matches(REGEX);
    assertThat(overrides.get("nessie.iceberg-base-uri")).matches(REGEX);
    assertThat(overrides.get("nessie.catalog-base-uri")).matches(REGEX);
  }
}

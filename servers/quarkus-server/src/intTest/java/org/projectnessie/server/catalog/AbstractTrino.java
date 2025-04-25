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
package org.projectnessie.server.catalog;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusTestProfile;
import java.io.InputStream;
import java.net.URI;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.projectnessie.nessie.testing.trino.TrinoContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

@SuppressWarnings("SqlNoDataSourceInspection")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public abstract class AbstractTrino {

  protected static TrinoContainer trinoContainer;

  // Need the S3 endpoint here, which is not accessible via '@BeforeAll'
  @BeforeEach
  void maybeStartTrino() throws Exception {
    if (trinoContainer != null) {
      return;
    }

    int quarkusPort = Integer.getInteger("quarkus.http.port", 19120);

    // Use Nessie's convenience 'trino-config' endpoint that provides a _starter_ catalog
    // configuration for Trino.
    Properties props = new Properties();
    try (InputStream in =
        tweakSetupConfigCall(
                URI.create(
                        format(
                            "http://localhost:%d/iceberg-ext/v1/client-template/trino",
                            quarkusPort))
                    .toURL()
                    .openConnection())
            .getInputStream()) {
      props.load(in);
    }

    for (String prop : List.of("s3.endpoint", "iceberg.rest-catalog.uri")) {
      props.put(
          prop,
          props
              .get(prop)
              .toString()
              .replace("localhost:", TrinoContainer.HOST_FROM_CONTAINER + ":"));
    }

    tweakTrinoProperties(props);

    trinoContainer = new TrinoContainer().withCatalog("nessie", props).withNetworkMode("bridge");
    try {
      trinoContainer.start();
    } catch (Exception e) {
      trinoContainer = null;
      throw e;
    }
  }

  protected void tweakTrinoProperties(Properties props) {}

  protected URLConnection tweakSetupConfigCall(URLConnection urlConnection) throws Exception {
    return urlConnection;
  }

  @AfterAll
  static void stopTrino() {
    try {
      if (trinoContainer != null) {
        trinoContainer.stop();
      }
    } finally {
      trinoContainer = null;
    }
  }

  @Test
  @Order(1)
  public void start() {
    // Just get Trino started - and separate all log messages from smoke()
    @Language("SQL")
    String showCatalogs = "SHOW CATALOGS";
    assertThat(trinoContainer.queryResults(showCatalogs))
        .extracting(l -> l.get(0).toString())
        .contains("memory")
        .contains("nessie");
  }

  @Test
  @Order(2)
  public void smoke() {
    @Language("SQL")
    String createSchema =
        """
        CREATE SCHEMA nessie.my_namespace
        """;
    assertThat(trinoContainer.queryResults(createSchema)).isEmpty();

    @Language("SQL")
    String showSchemas =
        """
        SHOW SCHEMAS FROM nessie
        """;
    assertThat(trinoContainer.queryResults(showSchemas))
        .extracting(l -> l.get(0).toString())
        // Trino 475 added the 'system' schema
        .contains("information_schema", "my_namespace");

    @Language("SQL")
    String createTable =
        """
        CREATE TABLE nessie.my_namespace.yearly_clicks (year, clicks)
          WITH (partitioning = ARRAY['year']) AS VALUES (2021, 10000), (2022, 20000)
        """;
    assertThat(trinoContainer.queryResults(createTable)).containsExactly(List.of(2L));

    @Language("SQL")
    String insert =
        """
        INSERT INTO nessie.my_namespace.yearly_clicks (year, clicks)
          VALUES (2023, 30000)
        """;
    assertThat(trinoContainer.queryResults(insert)).containsExactly(List.of(1L));

    @Language("SQL")
    String selectTable =
        """
        SELECT year, clicks FROM nessie.my_namespace.yearly_clicks
          ORDER BY year
        """;
    assertThat(trinoContainer.queryResults(selectTable))
        .containsExactly(List.of(2021, 10000), List.of(2022, 20000), List.of(2023, 30000));

    @Language("SQL")
    String createView =
        """
        CREATE VIEW nessie.my_namespace.my_view AS
          SELECT year, clicks FROM nessie.my_namespace.yearly_clicks
        """;
    assertThat(trinoContainer.queryResults(createView)).isEmpty();

    @Language("SQL")
    String selectView =
        """
          SELECT year, clicks FROM nessie.my_namespace.my_view
            ORDER BY year
          """;
    assertThat(trinoContainer.queryResults(selectView))
        .containsExactly(List.of(2021, 10000), List.of(2022, 20000), List.of(2023, 30000));
  }

  public static class BaseProfile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("nessie.catalog.service.s3.default-options.request-signing-enabled", "false")
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.enabled",
              "true") // Note: unused by Minio
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.assume-role",
              "test-role") // Note: unused by Minio
          .put(
              "nessie.catalog.service.s3.default-options.client-iam.external-id",
              "test-external-id") // Note: unused by Minio
          .build();
    }
  }
}

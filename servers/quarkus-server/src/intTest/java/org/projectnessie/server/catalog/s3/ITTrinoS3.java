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
package org.projectnessie.server.catalog.s3;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.nessie.testing.trino.TrinoContainer;
import org.projectnessie.nessie.testing.trino.TrinoResults;
import org.projectnessie.server.catalog.MinioTestResourceLifecycleManager;
import org.projectnessie.server.catalog.WarehouseLocation;
import org.testcontainers.Testcontainers;

@QuarkusTestResource(
    restrictToAnnotatedClass = true,
    value = MinioTestResourceLifecycleManager.class)
@QuarkusIntegrationTest
public class ITTrinoS3 {
  static TrinoContainer trinoContainer;

  @WarehouseLocation static URI warehouseLocation;

  static String icebergRestUri;

  @BeforeEach
  void maybeStartTrino() {
    if (trinoContainer != null) {
      return;
    }

    int quarkusPort = Integer.getInteger("quarkus.http.port", 19120);

    Testcontainers.exposeHostPorts(quarkusPort);

    icebergRestUri = format("http://host.testcontainers.internal:%d/iceberg/main", quarkusPort);

    trinoContainer = new TrinoContainer();
    trinoContainer.withAccessToHost(true);
    trinoContainer.withCatalog(
        "nessie",
        Map.of(
            "connector.name",
            "iceberg",
            "iceberg.catalog.type",
            "rest",
            "iceberg.rest-catalog.uri",
            icebergRestUri));
    trinoContainer.start();
  }

  @AfterAll
  static void stopTrino() {
    if (trinoContainer != null) {
      trinoContainer.stop();
    }
  }

  @Test
  public void smoke() {
    try (TrinoResults catalogs = trinoContainer.query("SHOW CATALOGS")) {
      assertThat(catalogs)
          .toIterable()
          .extracting(l -> l.get(0).toString())
          .contains("memory")
          .contains("nessie");
    }
    try (TrinoResults createSchema = trinoContainer.query("CREATE SCHEMA nessie.my_namespace")) {
      assertThat(createSchema).isExhausted();
    }
    try (TrinoResults schemas = trinoContainer.query("SHOW SCHEMAS FROM nessie")) {
      assertThat(schemas)
          .toIterable()
          .extracting(l -> l.get(0).toString())
          .containsExactlyInAnyOrder("information_schema", "my_namespace");
    }

    // Following fails with 'location must be set for my_namespace'
    // try (TrinoResults createTable =
    //       trinoContainer.query(
    //         """
    //           CREATE TABLE nessie.my_namespace.yearly_clicks (year, clicks)
    //             WITH (partitioning = ARRAY['year']) AS VALUES (2021, 10000), (2022, 20000)
    //           """)) {
    //  assertThat(createTable).isExhausted();
    // }
  }
}

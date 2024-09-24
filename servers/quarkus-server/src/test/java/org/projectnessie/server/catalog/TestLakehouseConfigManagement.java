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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.arc.Arc;
import io.quarkus.arc.ManagedContext;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.service.api.LakehouseConfigManagement;
import org.projectnessie.catalog.service.config.ImmutableCatalogConfig;
import org.projectnessie.catalog.service.config.ImmutableLakehouseConfig;
import org.projectnessie.catalog.service.config.ImmutableWarehouseConfig;
import org.projectnessie.catalog.service.config.LakehouseConfig;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;

@QuarkusTest
@TestProfile(TestLakehouseConfigManagement.Profile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestLakehouseConfigManagement {
  public static final String BUCKET = "bucket1";

  private HeapStorageBucket heapStorageBucket;
  private ObjectStorageMock.MockServer server;

  @Inject LakehouseConfigManagement configManagement;

  @BeforeEach
  public void setUp() {
    heapStorageBucket = HeapStorageBucket.newHeapStorageBucket();
    server =
        ObjectStorageMock.builder()
            .initAddress("localhost")
            .putBuckets(BUCKET, heapStorageBucket.bucket())
            .build()
            .start();
  }

  @AfterEach
  public void stop() {
    if (server != null) {
      try {
        server.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        server = null;
      }
    }
  }

  @Test
  @Order(1)
  public void initiallyEmpty() {
    LakehouseConfig current = configManagement.currentConfig();
    ImmutableLakehouseConfig empty =
        ImmutableLakehouseConfig.builder()
            .catalog(ImmutableCatalogConfig.builder().build())
            .s3(ImmutableS3Options.builder().build())
            .gcs(ImmutableGcsOptions.builder().build())
            .adls(ImmutableAdlsOptions.builder().build())
            .build();
    assertThat(current).isEqualTo(empty);

    // Verify for `@RequestScoped`
    ManagedContext rc = Arc.container().requestContext();
    rc.activate();
    try {
      LakehouseConfig configFromContainer = Arc.container().instance(LakehouseConfig.class).get();
      assertThat(ImmutableLakehouseConfig.builder().from(configFromContainer).build())
          .isEqualTo(empty);
    } finally {
      rc.terminate();
    }
  }

  @Test
  @Order(2)
  public void updateS3Bucket() {
    String s3Endpoint = server.getS3BaseUri().toString();

    LakehouseConfig current = configManagement.currentConfig();
    LakehouseConfig updated =
        ImmutableLakehouseConfig.builder()
            .from(current)
            .catalog(
                ImmutableCatalogConfig.builder()
                    .defaultWarehouse("my-warehouse")
                    .putWarehouse(
                        "my-warehouse",
                        ImmutableWarehouseConfig.builder().location("s3://" + BUCKET).build())
                    .build())
            .s3(
                ImmutableS3Options.builder()
                    .putBucket(
                        BUCKET,
                        ImmutableS3NamedBucketOptions.builder()
                            .endpoint(URI.create(s3Endpoint))
                            .region("us-east-1")
                            .pathStyleAccess(true)
                            .build())
                    .build())
            .build();

    configManagement.updateConfig(updated, current);

    LakehouseConfig actual = configManagement.currentConfig();
    assertThat(actual).isEqualTo(updated);

    // Verify for `@RequestScoped`
    ManagedContext rc = Arc.container().requestContext();
    rc.activate();
    try {
      LakehouseConfig configFromContainer = Arc.container().instance(LakehouseConfig.class).get();
      assertThat(ImmutableLakehouseConfig.builder().from(configFromContainer).build())
          .isEqualTo(updated);
    } finally {
      rc.terminate();
    }
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of("nessie.catalog.advanced.persist-config", "true");
    }
  }
}

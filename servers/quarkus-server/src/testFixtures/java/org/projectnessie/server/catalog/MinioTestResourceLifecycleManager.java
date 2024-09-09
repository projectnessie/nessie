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

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.URI;
import java.util.Map;
import org.projectnessie.minio.MinioContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class MinioTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  public static final String TEST_REGION = "us-east-1";

  private final MinioContainer minio =
      new MinioContainer().withRegion(TEST_REGION).withStartupAttempts(5);
  private URI warehouseLocation;
  private String scheme;

  @Override
  public void init(Map<String, String> initArgs) {
    scheme = initArgs.getOrDefault("scheme", "s3");
  }

  @Override
  public Map<String, String> start() {
    minio.start();
    warehouseLocation = minio.s3BucketUri(scheme, "");
    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.service.s3.default-options.endpoint", minio.s3endpoint())
        .put("nessie.catalog.service.s3.default-options.path-style-access", "true")
        .put("nessie.catalog.service.s3.default-options.sts-endpoint", minio.s3endpoint())
        .put("nessie.catalog.service.s3.default-options.region", TEST_REGION)
        .put(
            "nessie.catalog.service.s3.default-options.access-key",
            "urn:nessie-secret:quarkus:minio-access-key")
        .put("minio-access-key.name", minio.accessKey())
        .put("minio-access-key.secret", minio.secretKey())
        .put("nessie.catalog.default-warehouse", "warehouse")
        .put("nessie.catalog.warehouses.warehouse.location", warehouseLocation.toString())
        .build();
  }

  @Override
  public void stop() {
    minio.stop();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(minio, new TestInjector.MatchesType(MinioContainer.class));
    testInjector.injectIntoFields(
        URI.create(minio.s3endpoint()),
        new TestInjector.AnnotatedAndMatchesType(S3Endpoint.class, URI.class));
    testInjector.injectIntoFields(
        warehouseLocation,
        new TestInjector.AnnotatedAndMatchesType(WarehouseLocation.class, URI.class));
  }
}

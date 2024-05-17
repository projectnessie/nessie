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
import java.util.Map;
import org.projectnessie.minio.MinioContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class MinioTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  public static final String TEST_REGION = "us-east-1";

  private final MinioContainer minio =
      new MinioContainer().withRegion(TEST_REGION).withStartupAttempts(5);

  @Override
  public Map<String, String> start() {
    minio.start();
    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.service.s3.endpoint", minio.s3endpoint())
        .put("nessie.catalog.service.s3.path-style-access", "true")
        .put("nessie.catalog.service.s3.sts.endpoint", minio.s3endpoint())
        .put("nessie.catalog.service.s3.region", TEST_REGION)
        .put("nessie.catalog.service.s3.access-key-id-ref", "awsAccessKeyId")
        .put("nessie.catalog.service.s3.secret-access-key-ref", "awsSecretAccessKey")
        .put("nessie.catalog.secrets.awsAccessKeyId", minio.accessKey())
        .put("nessie.catalog.secrets.awsSecretAccessKey", minio.secretKey())
        .put("nessie.catalog.default-warehouse", "warehouse")
        .put("nessie.catalog.warehouses.warehouse.location", minio.s3BucketUri("").toString())
        .build();
  }

  @Override
  public void stop() {
    minio.stop();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(minio, new TestInjector.MatchesType(MinioContainer.class));
  }
}

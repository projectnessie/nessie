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

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.projectnessie.objectstoragemock.AccessCheckHandler;
import org.projectnessie.objectstoragemock.HeapStorageBucket;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.objectstoragemock.ObjectStorageMock.MockServer;
import org.projectnessie.objectstoragemock.sts.AssumeRoleHandler;
import org.projectnessie.objectstoragemock.sts.AssumeRoleResult;

public class ObjectStorageMockTestResourceLifecycleManager
    implements QuarkusTestResourceLifecycleManager {

  public static final String BUCKET = "bucket1";

  public static String bucketWarehouseLocation(String scheme) {
    return String.format("%s://%s/warehouse", scheme, BUCKET);
  }

  public static final String S3_WAREHOUSE_LOCATION = bucketWarehouseLocation("s3");
  public static final String S3A_WAREHOUSE_LOCATION = bucketWarehouseLocation("s3a");
  public static final String S3N_WAREHOUSE_LOCATION = bucketWarehouseLocation("s3n");
  public static final String GCS_WAREHOUSE_LOCATION = bucketWarehouseLocation("gs");
  public static final String ADLS_AUTHORITY = BUCKET + "@account.dfs.core.windows.net";
  public static final String ADLS_WAREHOUSE_LOCATION = "abfs://" + ADLS_AUTHORITY + "/warehouse";

  public static final String INIT_ADDRESS =
      "ObjectStorageMockTestResourceLifecycleManager.initAddress";

  private final AssumeRoleHandlerHolder assumeRoleHandler = new AssumeRoleHandlerHolder();
  private final AccessCheckHandlerHolder accessCheckHandler = new AccessCheckHandlerHolder();

  private HeapStorageBucket heapStorageBucket;
  private MockServer server;

  @Override
  public Map<String, String> start() {

    heapStorageBucket = HeapStorageBucket.newHeapStorageBucket();
    server =
        ObjectStorageMock.builder()
            .initAddress("localhost")
            .putBuckets(BUCKET, heapStorageBucket.bucket())
            .assumeRoleHandler(assumeRoleHandler)
            .accessCheckHandler(accessCheckHandler)
            .build()
            .start();

    String s3Endpoint = server.getS3BaseUri().toString();
    String gcsEndpoint = server.getGcsBaseUri().toString();
    String adlsEndpoint = server.getAdlsGen2BaseUri().resolve(BUCKET).toString();

    return ImmutableMap.<String, String>builder()
        // S3
        .put(
            "nessie.catalog.service.s3.default-options.sts-endpoint",
            server.getStsEndpointURI().toString())
        .put("nessie.catalog.service.s3.buckets.mock-bucket.name", BUCKET)
        .put("nessie.catalog.service.s3.buckets.mock-bucket.endpoint", s3Endpoint)
        .put("nessie.catalog.service.s3.buckets.mock-bucket.region", "us-east-1")
        .put("nessie.catalog.service.s3.buckets.mock-bucket.path-style-access", "true")
        .put(
            "nessie.catalog.service.s3.buckets.mock-bucket.access-key",
            "urn:nessie-secret:quarkus:mock-bucket-access-key")
        .put("mock-bucket-access-key.name", "accessKey")
        .put("mock-bucket-access-key.secret", "secretKey")
        // GCS
        .put("nessie.catalog.service.gcs.buckets.mock-bucket.name", BUCKET)
        .put("nessie.catalog.service.gcs.buckets.mock-bucket.host", gcsEndpoint)
        .put("nessie.catalog.service.gcs.buckets.mock-bucket.project-id", "my-project")
        .put("nessie.catalog.service.gcs.buckets.mock-bucket.auth-type", "none")
        // ADLS
        .put("nessie.catalog.service.adls.file-systems.mock-fs.name", BUCKET)
        .put("nessie.catalog.service.adls.file-systems.mock-fs.authority", ADLS_AUTHORITY)
        .put("nessie.catalog.service.adls.file-systems.mock-fs.endpoint", adlsEndpoint)
        .put(
            "nessie.catalog.service.adls.file-systems.mock-fs.sas-token",
            "urn:nessie-secret:quarkus:sas-token")
        .put("sas-token.key", "sas-token")
        .put("nessie.catalog.service.adls.file-systems.mock-fs.auth-type", "SAS_TOKEN")
        .build();
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        heapStorageBucket, new TestInjector.MatchesType(HeapStorageBucket.class));

    testInjector.injectIntoFields(
        assumeRoleHandler, new TestInjector.MatchesType(AssumeRoleHandlerHolder.class));

    testInjector.injectIntoFields(
        accessCheckHandler, new TestInjector.MatchesType(AccessCheckHandlerHolder.class));
  }

  @Override
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

  public static final class AssumeRoleHandlerHolder implements AssumeRoleHandler {
    private final AtomicReference<AssumeRoleHandler> handler = new AtomicReference<>();

    public void set(AssumeRoleHandler handler) {
      this.handler.set(handler);
    }

    @Override
    public AssumeRoleResult assumeRole(
        String action,
        String version,
        String roleArn,
        String roleSessionName,
        String policy,
        Integer durationSeconds,
        String externalId,
        String serialNumber) {
      return handler
          .get()
          .assumeRole(
              action,
              version,
              roleArn,
              roleSessionName,
              policy,
              durationSeconds,
              externalId,
              serialNumber);
    }
  }

  public static final class AccessCheckHandlerHolder implements AccessCheckHandler {
    private final AtomicReference<AccessCheckHandler> handler = new AtomicReference<>();

    public void set(AccessCheckHandler handler) {
      this.handler.set(handler);
    }

    @Override
    public boolean accessAllowed(String objectKey) {
      AccessCheckHandler delegate = handler.get();
      return delegate == null || delegate.accessAllowed(objectKey);
    }
  }
}

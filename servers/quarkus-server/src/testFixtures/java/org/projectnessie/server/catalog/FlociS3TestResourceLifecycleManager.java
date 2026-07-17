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
import org.projectnessie.testing.floci.s3.FlociS3Container;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;

public class FlociS3TestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {

  public static final String TEST_REGION = "us-east-1";
  public static final String TEST_ACCOUNT_ID = "123456789012";
  public static final String SERVER_ROLE_ARN =
      "arn:aws:iam::" + TEST_ACCOUNT_ID + ":role/server-role";
  public static final String CLIENT_ROLE_ARN =
      "arn:aws:iam::" + TEST_ACCOUNT_ID + ":role/client-role";

  private static final String ASSUME_ROLE_POLICY =
      """
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Principal": { "AWS": "*" },
            "Action": "sts:AssumeRole"
          }
        ]
      }
      """;

  private static final String ALLOW_S3_POLICY =
      """
      {
        "Version": "2012-10-17",
        "Statement": [
          {
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::*"
          }
        ]
      }
      """;

  private final FlociS3Container flociS3 =
      new FlociS3Container(null, "test", "test", null)
          .withRegion(TEST_REGION)
          .withDefaultAccountId(TEST_ACCOUNT_ID)
          .withIamEnforcement()
          .withStartupAttempts(5);
  private URI warehouseLocation;
  private String scheme;

  @Override
  public void init(Map<String, String> initArgs) {
    scheme = initArgs.getOrDefault("scheme", "s3");
  }

  @Override
  public Map<String, String> start() {
    flociS3.start();
    seedIamRoles();
    warehouseLocation = flociS3.s3BucketUri(scheme, "");
    return ImmutableMap.<String, String>builder()
        .put("nessie.catalog.service.s3.default-options.endpoint", flociS3.s3endpoint())
        .put("nessie.catalog.service.s3.default-options.path-style-access", "true")
        .put("nessie.catalog.service.s3.default-options.sts-endpoint", flociS3.s3endpoint())
        .put("nessie.catalog.service.s3.default-options.region", TEST_REGION)
        .put(
            "nessie.catalog.service.s3.default-options.access-key",
            "urn:nessie-secret:quarkus:flociS3-access-key")
        .put("flociS3-access-key.name", flociS3.accessKey())
        .put("flociS3-access-key.secret", flociS3.secretKey())
        .put("nessie.catalog.default-warehouse", "warehouse")
        .put("nessie.catalog.warehouses.warehouse.location", warehouseLocation.toString())
        .build();
  }

  @Override
  public void stop() {
    flociS3.stop();
  }

  private void seedIamRoles() {
    try (IamClient iam =
        IamClient.builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .endpointOverride(URI.create(flociS3.s3endpoint()))
            .region(Region.of(TEST_REGION))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(flociS3.accessKey(), flociS3.secretKey())))
            .build()) {
      seedRole(iam, "server-role");
      seedRole(iam, "client-role");
    }
  }

  private static void seedRole(IamClient iam, String roleName) {
    iam.createRole(
        CreateRoleRequest.builder()
            .roleName(roleName)
            .assumeRolePolicyDocument(ASSUME_ROLE_POLICY)
            .build());
    iam.putRolePolicy(
        PutRolePolicyRequest.builder()
            .roleName(roleName)
            .policyName("allow-s3")
            .policyDocument(ALLOW_S3_POLICY)
            .build());
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(flociS3, new TestInjector.MatchesType(FlociS3Container.class));
    testInjector.injectIntoFields(
        URI.create(flociS3.s3endpoint()),
        new TestInjector.AnnotatedAndMatchesType(S3Endpoint.class, URI.class));
    testInjector.injectIntoFields(
        warehouseLocation,
        new TestInjector.AnnotatedAndMatchesType(WarehouseLocation.class, URI.class));
  }
}

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
package org.projectnessie.catalog.files.s3;

import static org.projectnessie.catalog.files.s3.S3IamPolicies.locationDependentPolicy;
import static org.projectnessie.catalog.files.s3.S3Utils.newCredentialsProvider;

import java.util.Optional;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.S3AuthType;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3ClientIam;
import org.projectnessie.catalog.files.config.S3Iam;
import org.projectnessie.catalog.files.config.S3ServerIam;
import org.projectnessie.catalog.secrets.SecretsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

class StsCredentialsFetcherImpl implements StsCredentialsFetcher {

  private final StsClientsPool clientsPool;
  private final SecretsProvider secretsProvider;

  StsCredentialsFetcherImpl(StsClientsPool clientsPool, SecretsProvider secretsProvider) {
    this.clientsPool = clientsPool;
    this.secretsProvider = secretsProvider;
  }

  @Override
  public Credentials fetchCredentialsForClient(
      S3BucketOptions bucketOptions, S3ClientIam iam, Optional<StorageLocations> locations) {
    AssumeRoleRequest.Builder request = AssumeRoleRequest.builder();
    locations.ifPresent(
        storageLocations -> request.policy(locationDependentPolicy(iam, storageLocations)));
    return doFetchCredentials(bucketOptions, request, iam);
  }

  @Override
  public Credentials fetchCredentialsForServer(S3BucketOptions bucketOptions, S3ServerIam iam) {
    return doFetchCredentials(bucketOptions, AssumeRoleRequest.builder(), iam);
  }

  private Credentials doFetchCredentials(
      S3BucketOptions bucketOptions, AssumeRoleRequest.Builder request, S3Iam iam) {
    request.roleSessionName(iam.roleSessionName().orElse(S3Iam.DEFAULT_SESSION_NAME));
    iam.policy().ifPresent(request::policy);
    iam.assumeRole().ifPresent(request::roleArn);
    iam.externalId().ifPresent(request::externalId);
    iam.sessionDuration()
        .ifPresent(duration -> request.durationSeconds((int) duration.toSeconds()));
    request.overrideConfiguration(
        builder -> {
          S3AuthType authType = bucketOptions.effectiveAuthType();
          builder.credentialsProvider(
              newCredentialsProvider(authType, bucketOptions, secretsProvider));
        });

    AssumeRoleRequest req = request.build();

    @SuppressWarnings("resource")
    StsClient client = clientsPool.stsClientForBucket(bucketOptions);

    AssumeRoleResponse response = client.assumeRole(req);
    return response.credentials();
  }
}

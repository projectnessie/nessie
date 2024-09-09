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

import java.util.function.Supplier;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3Sessions {

  private final String repositoryId;
  private final StsCredentialsManager sessionsManager;

  public S3Sessions(String repositoryId, StsCredentialsManager sessionsManager) {
    this.repositoryId = repositoryId;
    this.sessionsManager = sessionsManager;
  }

  /**
   * Returns session credentials suitable for the catalog server itself when accessing the given
   * bucket.
   */
  AwsCredentialsProvider assumeRoleForServer(S3BucketOptions options) {
    return credentials(() -> sessionsManager.sessionCredentialsForServer(repositoryId, options));
  }

  /**
   * Returns session credentials suitable for catalog clients, when accessing the given bucket, and
   * with policies enforcing only access to the given locations.
   */
  AwsCredentialsProvider assumeRoleForClient(S3BucketOptions options, StorageLocations locations) {
    return credentials(() -> sessionsManager.sessionCredentialsForClient(options, locations));
  }

  private AwsCredentialsProvider credentials(Supplier<Credentials> supplier) {
    return () -> {
      Credentials credentials = supplier.get();
      return AwsSessionCredentials.builder()
          .accessKeyId(credentials.accessKeyId())
          .secretAccessKey(credentials.secretAccessKey())
          .sessionToken(credentials.sessionToken())
          .expirationTime(credentials.expiration())
          .build();
    };
  }
}

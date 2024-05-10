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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.services.sts.model.Credentials;

public class S3Sessions {

  private final String repositoryId;
  private final S3SessionsManager sessionsManager;

  public S3Sessions(String repositoryId, S3SessionsManager sessionsManager) {
    this.repositoryId = repositoryId;
    this.sessionsManager = sessionsManager;
  }

  /**
   * Returns potentially shared session credentials for the specified role using the default session
   * duration.
   */
  AwsCredentialsProvider assumeRoleForServer(S3BucketOptions options) {
    return credentials(
        options, () -> sessionsManager.sessionCredentialsForServer(repositoryId, options));
  }

  /**
   * Returns session credentials for the specified role and the expected session duration. Note:
   * credentials returned from this method are generally not shared across sessions.
   */
  AwsCredentialsProvider assumeRoleForClient(S3BucketOptions options) {
    return credentials(
        options, () -> sessionsManager.sessionCredentialsForClient(repositoryId, options));
  }

  private AwsCredentialsProvider credentials(
      S3BucketOptions options, Supplier<Credentials> supplier) {
    if (options.cloud().orElse(null) != Cloud.AMAZON) {
      if (options.stsEndpoint().isEmpty()) {
        throw new IllegalArgumentException(
            "STS endpoint must be provided for cloud: "
                + options.cloud().map(Cloud::name).orElse("<unset>"));
      }
    }

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

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

import java.util.Optional;
import org.projectnessie.catalog.secrets.BasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;

public class S3Clients {

  /** Builds an SDK Http client based on the Apache Http client. */
  public static SdkHttpClient apacheHttpClient(S3Config s3Config) {
    ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder();
    s3Config.maxHttpConnections().ifPresent(httpClient::maxConnections);
    s3Config.readTimeout().ifPresent(httpClient::socketTimeout);
    s3Config.connectTimeout().ifPresent(httpClient::connectionTimeout);
    s3Config.connectionAcquisitionTimeout().ifPresent(httpClient::connectionAcquisitionTimeout);
    s3Config.connectionMaxIdleTime().ifPresent(httpClient::connectionMaxIdleTime);
    s3Config.connectionTimeToLive().ifPresent(httpClient::connectionTimeToLive);
    s3Config.expectContinueEnabled().ifPresent(httpClient::expectContinueEnabled);
    return httpClient.build();
  }

  public static AwsCredentialsProvider basicCredentialsProvider(
      Optional<BasicCredentials> accessKey) {
    return accessKey
        .map(key -> AwsBasicCredentials.create(key.name(), key.secret()))
        .map(creds -> (AwsCredentialsProvider) StaticCredentialsProvider.create(creds))
        .orElse(DefaultCredentialsProvider.create());
  }

  public static AwsCredentialsProvider awsCredentialsProvider(
      S3BucketOptions bucketOptions, S3Sessions sessions) {
    Optional<String> role = bucketOptions.assumeRole();
    if (role.isEmpty()) {
      return basicCredentialsProvider(bucketOptions.accessKey());
    }
    return sessions.assumeRoleForServer(bucketOptions);
  }
}

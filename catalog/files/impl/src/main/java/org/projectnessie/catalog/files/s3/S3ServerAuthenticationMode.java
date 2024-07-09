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

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;

/**
 * @see <a
 *     href="https://github.com/quarkiverse/quarkus-amazon-services/blob/4ecc71767857d476767ffd36f0fac98c1b2b7de1/common/runtime/src/main/java/io/quarkus/amazon/common/runtime/AwsCredentialsProviderType.java">Quarkus
 *     AWS AwsCredentialsProviderType</a>
 */
public enum S3ServerAuthenticationMode {
  DEFAULT {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return DefaultCredentialsProvider.builder()
          .asyncCredentialUpdateEnabled(false)
          .reuseLastProviderEnabled(true)
          .build();
    }
  },

  STATIC {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return bucketOptions
          .accessKey()
          // TODO support session token: AwsSessionCredentials.create(accessKeyId, secretAccessKey,
          // sessionToken)
          .map(key -> AwsBasicCredentials.create(key.name(), key.secret()))
          .map(creds -> (AwsCredentialsProvider) StaticCredentialsProvider.create(creds))
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "Missing access key and secret for STATIC auth type"));
    }
  },

  SYSTEM_PROPERTY {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return SystemPropertyCredentialsProvider.create();
    }
  },

  ENV_VARIABLE {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return EnvironmentVariableCredentialsProvider.create();
    }
  },

  PROFILE {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return bucketOptions
          .profile()
          .map(ProfileCredentialsProvider::create)
          .orElseThrow(
              () -> new IllegalArgumentException("Missing profile name for PROFILE auth type"));
    }
  },

  CONTAINER {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return ContainerCredentialsProvider.builder().build();
    }
  },

  INSTANCE_PROFILE {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return InstanceProfileCredentialsProvider.builder().build();
    }
  },

  ANONYMOUS {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return AnonymousCredentialsProvider.create();
    }
  },
  ;

  public abstract AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions);
}

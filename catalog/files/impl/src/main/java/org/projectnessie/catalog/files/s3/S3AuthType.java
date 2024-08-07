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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/** Server authentication modes for S3. */
public enum S3AuthType {
  APPLICATION_GLOBAL {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return DefaultCredentialsProvider.create(); // actually a singleton
    }
  },

  STATIC {
    @Override
    public AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions) {
      return bucketOptions
          .accessKey()
          .map(key -> AwsBasicCredentials.create(key.name(), key.secret()))
          .map(creds -> (AwsCredentialsProvider) StaticCredentialsProvider.create(creds))
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "Missing access key and secret for STATIC authentication mode"));
    }
  },
  ;

  public abstract AwsCredentialsProvider newCredentialsProvider(S3BucketOptions bucketOptions);
}

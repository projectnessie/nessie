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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.time.Duration;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface S3ProgrammaticOptions extends S3Options<S3BucketOptions> {
  Map<String, S3BucketOptions> buckets();

  static Builder builder() {
    return ImmutableS3ProgrammaticOptions.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder cloud(Cloud cloud);

    @CanIgnoreReturnValue
    Builder endpoint(URI endpoint);

    @CanIgnoreReturnValue
    Builder externalEndpoint(URI externalEndpoint);

    @CanIgnoreReturnValue
    Builder region(String region);

    @CanIgnoreReturnValue
    Builder roleArn(String roleArn);

    @CanIgnoreReturnValue
    Builder pathStyleAccess(boolean pathStyleAccess);

    @CanIgnoreReturnValue
    Builder projectId(String projectId);

    @CanIgnoreReturnValue
    Builder accessKeyIdRef(String accessKeyIdRef);

    @CanIgnoreReturnValue
    Builder secretAccessKeyRef(String secretAccessKeyRef);

    @CanIgnoreReturnValue
    Builder sessionCredentialCacheMaxEntries(int sessionCredentialCacheMaxEntries);

    @CanIgnoreReturnValue
    Builder sessionCredentialRefreshGracePeriod(Duration sessionCredentialRefreshGracePeriod);

    @CanIgnoreReturnValue
    Builder stsClientsCacheMaxEntries(int stsClientsCacheMaxEntries);

    @CanIgnoreReturnValue
    Builder externalId(String externalId);

    @CanIgnoreReturnValue
    Builder putBuckets(String bucket, S3BucketOptions bucketOptions);

    @CanIgnoreReturnValue
    Builder putAllBuckets(Map<String, ? extends S3BucketOptions> perBucketOptions);

    @CanIgnoreReturnValue
    Builder buckets(Map<String, ? extends S3BucketOptions> bucketOptions);

    S3ProgrammaticOptions build();
  }

  @Value.Immutable
  interface S3PerBucketOptions extends S3BucketOptions {

    static Builder builder() {
      return ImmutableS3PerBucketOptions.builder();
    }

    interface Builder {
      @CanIgnoreReturnValue
      Builder from(S3BucketOptions instance);

      @CanIgnoreReturnValue
      Builder cloud(Cloud cloud);

      @CanIgnoreReturnValue
      Builder endpoint(URI endpoint);

      @CanIgnoreReturnValue
      Builder externalEndpoint(URI externalEndpoint);

      @CanIgnoreReturnValue
      Builder region(String region);

      @CanIgnoreReturnValue
      Builder pathStyleAccess(boolean pathStyleAccess);

      @CanIgnoreReturnValue
      Builder projectId(String projectId);

      @CanIgnoreReturnValue
      Builder accessKeyIdRef(String accessKeyIdRef);

      @CanIgnoreReturnValue
      Builder secretAccessKeyRef(String secretAccessKeyRef);

      @CanIgnoreReturnValue
      Builder accessPoint(String accessPoint);

      @CanIgnoreReturnValue
      Builder allowCrossRegionAccessPoint(boolean allowCrossRegionAccessPoint);

      @CanIgnoreReturnValue
      Builder stsEndpoint(URI stsEndpoint);

      @CanIgnoreReturnValue
      Builder roleArn(String roleArn);

      @CanIgnoreReturnValue
      Builder iamPolicy(String iamPolicy);

      @CanIgnoreReturnValue
      Builder roleSessionName(String roleSessionName);

      @CanIgnoreReturnValue
      Builder externalId(String externalId);

      S3PerBucketOptions build();
    }
  }
}

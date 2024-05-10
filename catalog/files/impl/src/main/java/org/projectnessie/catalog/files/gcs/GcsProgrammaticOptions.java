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
package org.projectnessie.catalog.files.gcs;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface GcsProgrammaticOptions extends GcsOptions<GcsBucketOptions> {
  @Override
  Map<String, GcsBucketOptions> buckets();

  static Builder builder() {
    return ImmutableGcsProgrammaticOptions.builder();
  }

  interface Builder {
    @CanIgnoreReturnValue
    Builder putBuckets(String bucket, GcsBucketOptions bucketOptions);

    @CanIgnoreReturnValue
    Builder putAllBuckets(Map<String, ? extends GcsBucketOptions> perBucketOptions);

    @CanIgnoreReturnValue
    Builder buckets(Map<String, ? extends GcsBucketOptions> bucketOptions);

    @CanIgnoreReturnValue
    Builder host(URI host);

    @CanIgnoreReturnValue
    Builder externalHost(URI externalHost);

    @CanIgnoreReturnValue
    Builder projectId(String projectId);

    @CanIgnoreReturnValue
    Builder quotaProjectId(String quotaProjectId);

    @CanIgnoreReturnValue
    Builder clientLibToken(String clientLibToken);

    @CanIgnoreReturnValue
    Builder authType(GcsAuthType authType);

    @CanIgnoreReturnValue
    Builder authCredentialsJsonRef(String authCredentialsJsonRef);

    @CanIgnoreReturnValue
    Builder oauth2TokenRef(String oauth2tokenRef);

    @CanIgnoreReturnValue
    Builder oauth2TokenExpiresAt(Instant oauth2TokenExpiresAt);

    @CanIgnoreReturnValue
    Builder maxAttempts(int maxAttempts);

    @CanIgnoreReturnValue
    Builder logicalTimeout(Duration logicalTimeout);

    @CanIgnoreReturnValue
    Builder totalTimeout(Duration totalTimeout);

    @CanIgnoreReturnValue
    Builder initialRetryDelay(Duration initialRetryDelay);

    @CanIgnoreReturnValue
    Builder maxRetryDelay(Duration maxRetryDelay);

    @CanIgnoreReturnValue
    Builder retryDelayMultiplier(double retryDelayMultiplier);

    @CanIgnoreReturnValue
    Builder initialRpcTimeout(Duration initialRpcTimeout);

    @CanIgnoreReturnValue
    Builder maxRpcTimeout(Duration maxRpcTimeout);

    @CanIgnoreReturnValue
    Builder rpcTimeoutMultiplier(double rpcTimeoutMultiplier);

    @CanIgnoreReturnValue
    Builder readChunkSize(int readChunkSize);

    @CanIgnoreReturnValue
    Builder writeChunkSize(int writeChunkSize);

    @CanIgnoreReturnValue
    Builder encryptionKeyRef(String encryptionKeyRef);

    @CanIgnoreReturnValue
    Builder userProject(String userProject);

    @CanIgnoreReturnValue
    Builder readTimeout(Duration readTimeout);

    @CanIgnoreReturnValue
    Builder connectTimeout(Duration connectTimeout);

    GcsProgrammaticOptions build();
  }

  @Value.Immutable
  interface GcsPerBucketOptions extends GcsBucketOptions {

    static Builder builder() {
      return ImmutableGcsPerBucketOptions.builder();
    }

    interface Builder {
      @CanIgnoreReturnValue
      Builder from(GcsBucketOptions instance);

      @CanIgnoreReturnValue
      Builder host(URI host);

      @CanIgnoreReturnValue
      Builder externalHost(URI externalHost);

      @CanIgnoreReturnValue
      Builder projectId(String projectId);

      @CanIgnoreReturnValue
      Builder quotaProjectId(String quotaProjectId);

      @CanIgnoreReturnValue
      Builder clientLibToken(String clientLibToken);

      @CanIgnoreReturnValue
      Builder authType(GcsAuthType authType);

      @CanIgnoreReturnValue
      Builder authCredentialsJsonRef(String authCredentialsJsonRef);

      @CanIgnoreReturnValue
      Builder oauth2TokenRef(String oauth2tokenRef);

      @CanIgnoreReturnValue
      Builder oauth2TokenExpiresAt(Instant oauth2TokenExpiresAt);

      @CanIgnoreReturnValue
      Builder maxAttempts(int maxAttempts);

      @CanIgnoreReturnValue
      Builder logicalTimeout(Duration logicalTimeout);

      @CanIgnoreReturnValue
      Builder totalTimeout(Duration totalTimeout);

      @CanIgnoreReturnValue
      Builder initialRetryDelay(Duration initialRetryDelay);

      @CanIgnoreReturnValue
      Builder maxRetryDelay(Duration maxRetryDelay);

      @CanIgnoreReturnValue
      Builder retryDelayMultiplier(double retryDelayMultiplier);

      @CanIgnoreReturnValue
      Builder initialRpcTimeout(Duration initialRpcTimeout);

      @CanIgnoreReturnValue
      Builder maxRpcTimeout(Duration maxRpcTimeout);

      @CanIgnoreReturnValue
      Builder rpcTimeoutMultiplier(double rpcTimeoutMultiplier);

      @CanIgnoreReturnValue
      Builder readChunkSize(int readChunkSize);

      @CanIgnoreReturnValue
      Builder writeChunkSize(int writeChunkSize);

      @CanIgnoreReturnValue
      Builder encryptionKeyRef(String encryptionKeyRef);

      @CanIgnoreReturnValue
      Builder userProject(String userProject);

      @CanIgnoreReturnValue
      Builder readTimeout(Duration readTimeout);

      @CanIgnoreReturnValue
      Builder connectTimeout(Duration connectTimeout);

      GcsPerBucketOptions build();
    }
  }
}

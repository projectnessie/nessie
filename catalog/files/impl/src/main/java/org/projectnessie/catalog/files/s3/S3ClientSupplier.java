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

import static com.google.common.base.Preconditions.checkArgument;
import static org.projectnessie.catalog.files.s3.S3Clients.serverCredentialsProvider;
import static org.projectnessie.catalog.files.s3.S3Utils.isS3scheme;

import java.io.InputStream;
import java.net.URI;
import java.util.Optional;
import java.util.function.Function;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3NamedBucketOptions;
import org.projectnessie.catalog.files.config.S3Options;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkField;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFile.Type;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.S3Request;
import software.amazon.awssdk.services.s3.model.S3Request.Builder;

public class S3ClientSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ClientSupplier.class);
  private static final ProfileFile EMPTY_PROFILE_FILE =
      ProfileFile.builder().content(InputStream.nullInputStream()).type(Type.CONFIGURATION).build();

  private final SdkHttpClient sdkClient;
  private final S3Options s3options;
  private final S3Sessions sessions;
  private final SecretsProvider secretsProvider;

  public S3ClientSupplier(
      SdkHttpClient sdkClient,
      S3Options s3options,
      S3Sessions sessions,
      SecretsProvider secretsProvider) {
    this.sdkClient = sdkClient;
    this.s3options = s3options;
    this.sessions = sessions;
    this.secretsProvider = secretsProvider;
  }

  S3Options s3options() {
    return s3options;
  }

  /**
   * Produces an S3 client for the set of S3 options and secrets. S3 options are retrieved from the
   * per-bucket config, which derives from the global config. References to the secrets that contain
   * the actual S3 access-key-ID and secret-access-key are present in the S3 options as well.
   */
  public S3Client getClient(StorageUri location) {

    String scheme = location.scheme();
    checkArgument(isS3scheme(scheme), "Invalid S3 scheme: %s", location);

    S3NamedBucketOptions bucketOptions = s3options.resolveOptionsForUri(location);
    return getClient(bucketOptions);
  }

  public S3Client getClient(S3NamedBucketOptions bucketOptions) {
    S3ClientBuilder builder =
        S3Client.builder()
            .httpClient(sdkClient)
            .credentialsProvider(
                serverCredentialsProvider(bucketOptions, sessions, secretsProvider))
            .overrideConfiguration(
                override -> override.defaultProfileFileSupplier(() -> EMPTY_PROFILE_FILE))
            .serviceConfiguration(
                serviceConfig -> configureServiceConfiguration(bucketOptions, serviceConfig));

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(
          "Building S3-client for bucket named '{}' using endpoint {} with {}",
          bucketOptions.name().orElseThrow(),
          bucketOptions.endpoint(),
          toLogString(bucketOptions));
    }

    bucketOptions.endpoint().ifPresent(builder::endpointOverride);
    bucketOptions.region().map(Region::of).ifPresent(builder::region);
    bucketOptions.pathStyleAccess().ifPresent(builder::forcePathStyle);
    bucketOptions
        .allowCrossRegionAccessPoint()
        .ifPresent(cr -> builder.disableMultiRegionAccessPoints(!cr));

    S3Client s3Client = builder.build();

    if (bucketOptions.accessPoint().isPresent()) {
      String accessPoint = bucketOptions.accessPoint().get();
      s3Client = new AccessPointAwareS3Client(s3Client, accessPoint);
    }

    return s3Client;
  }

  static void configureServiceConfiguration(
      S3BucketOptions bucketOptions, S3Configuration.Builder serviceConfig) {
    serviceConfig.profileFile(() -> EMPTY_PROFILE_FILE);
    bucketOptions.chunkedEncodingEnabled().ifPresent(serviceConfig::chunkedEncodingEnabled);
  }

  private static String toLogString(S3BucketOptions options) {
    return "S3BucketOptions{"
        + "endpoint="
        + options.endpoint().map(URI::toString).orElse("<undefined>")
        + ", region="
        + options.region().orElse("<undefined>")
        + "}";
  }

  private static class AccessPointAwareS3Client extends DelegatingS3Client {

    private final String accessPoint;

    public AccessPointAwareS3Client(S3Client s3Client, String accessPoint) {
      super(s3Client);
      this.accessPoint = accessPoint;
    }

    @Override
    protected <T extends S3Request, ReturnT> ReturnT invokeOperation(
        T request, Function<T, ReturnT> operation) {
      Optional<SdkField<?>> bucket =
          request.sdkFields().stream()
              .filter(f -> f.memberName().equalsIgnoreCase("Bucket"))
              .findFirst();
      if (bucket.isPresent()) {
        Builder builder = request.toBuilder();
        bucket.get().set(builder, accessPoint);
        @SuppressWarnings("unchecked")
        T modified = (T) builder.build();
        return super.invokeOperation(modified, operation);
      }
      return super.invokeOperation(request, operation);
    }
  }
}

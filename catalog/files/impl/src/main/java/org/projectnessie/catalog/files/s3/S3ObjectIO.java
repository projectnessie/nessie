/*
 * Copyright (C) 2023 Dremio
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
import static org.projectnessie.catalog.files.s3.S3Utils.isS3scheme;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3NamedBucketOptions;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectIO implements ObjectIO {

  static final String S3_CLIENT_REGION = "client.region";
  static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  static final String S3_SESSION_TOKEN = "s3.session-token";
  static final String S3_ENDPOINT = "s3.endpoint";
  static final String S3_ACCESS_POINTS_PREFIX = "s3.access-points.";
  static final String S3_PATH_STYLE_ACCESS = "s3.path-style-access";
  static final String S3_USE_ARN_REGION_ENABLED = "s3.use-arn-region-enabled";
  static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";
  static final String S3_SIGNER = "s3.signer";

  private final S3ClientSupplier s3clientSupplier;
  private final S3CredentialsResolver s3CredentialsResolver;

  public S3ObjectIO(
      S3ClientSupplier s3clientSupplier, S3CredentialsResolver s3CredentialsResolver) {
    this.s3clientSupplier = s3clientSupplier;
    this.s3CredentialsResolver = s3CredentialsResolver;
  }

  @Override
  public void ping(StorageUri uri) {
    S3Client s3client = s3clientSupplier.getClient(uri);
    s3client.headBucket(b -> b.bucket(uri.requiredAuthority()));
  }

  @Override
  public InputStream readObject(StorageUri uri) {
    checkArgument(uri != null, "Invalid location: null");
    String scheme = uri.scheme();
    checkArgument(isS3scheme(scheme), "Invalid S3 scheme: %s", uri);

    S3Client s3client = s3clientSupplier.getClient(uri);

    return s3client.getObject(
        GetObjectRequest.builder()
            .bucket(uri.requiredAuthority())
            .key(withoutLeadingSlash(uri))
            .build());
  }

  @Override
  public OutputStream writeObject(StorageUri uri) {
    checkArgument(uri != null, "Invalid location: null");
    checkArgument(isS3scheme(uri.scheme()), "Invalid S3 scheme: %s", uri);

    return new ByteArrayOutputStream() {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
          super.close();

          S3Client s3client = s3clientSupplier.getClient(uri);

          s3client.putObject(
              PutObjectRequest.builder()
                  .bucket(uri.requiredAuthority())
                  .key(withoutLeadingSlash(uri))
                  .build(),
              RequestBody.fromBytes(toByteArray()));
        }
      }
    };
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) {
    // TODO this should group by **resolved prefix**
    Map<String, List<StorageUri>> bucketToUris =
        uris.stream().collect(Collectors.groupingBy(StorageUri::requiredAuthority));

    for (Map.Entry<String, List<StorageUri>> bucketDeletes : bucketToUris.entrySet()) {
      String bucket = bucketDeletes.getKey();
      List<StorageUri> locations = bucketDeletes.getValue();

      S3Client s3client = s3clientSupplier.getClient(locations.get(0));

      List<ObjectIdentifier> objectIdentifiers =
          locations.stream()
              .map(S3ObjectIO::withoutLeadingSlash)
              .map(key -> ObjectIdentifier.builder().key(key).build())
              .collect(Collectors.toList());

      DeleteObjectsRequest.Builder deleteObjectsRequest =
          DeleteObjectsRequest.builder()
              .bucket(bucket)
              .delete(Delete.builder().objects(objectIdentifiers).build());

      s3client.deleteObjects(deleteObjectsRequest.build());
    }
  }

  @Override
  public Optional<String> canResolve(StorageUri uri) {
    String scheme = uri.scheme();
    if (!isS3scheme(scheme)) {
      return Optional.of("Not an S3 URI");
    }

    try {
      S3Client s3client = s3clientSupplier.getClient(uri);
      return s3client != null
          ? Optional.empty()
          : Optional.of("S3 client could not be constructed");
    } catch (IllegalArgumentException e) {
      return Optional.of(e.getMessage());
    }
  }

  private static String withoutLeadingSlash(StorageUri uri) {
    String path = uri.requiredPath();
    return path.startsWith("/") ? path.substring(1) : path;
  }

  @Override
  public void configureIcebergWarehouse(
      StorageUri warehouse,
      BiConsumer<String, String> defaultConfig,
      BiConsumer<String, String> configOverride) {
    icebergConfigDefaults(warehouse, defaultConfig);
    icebergConfigOverrides(warehouse, configOverride);
  }

  @Override
  public void configureIcebergTable(
      StorageLocations storageLocations,
      BiConsumer<String, String> config,
      Predicate<Duration> enableRequestSigning,
      boolean canDoCredentialsVending) {
    if (Stream.concat(
            storageLocations.writeableLocations().stream(),
            storageLocations.readonlyLocations().stream())
        .map(StorageUri::scheme)
        .noneMatch(S3Utils::isS3scheme)) {
      // If there's no S3 location, no need to do any S3 setup.
      // This is also for test-cases that do not have a "proper" table location, e.g. a hard-coded
      // string `table-location`.
      return;
    }

    icebergConfigDefaults(storageLocations.warehouseLocation(), config);
    S3BucketOptions bucketOptions =
        icebergConfigOverrides(storageLocations.warehouseLocation(), config);

    // Note: 'accessDelegationPredicate' returns 'true', if the client did not send the
    // 'X-Iceberg-Access-Delegation' header (or if the header contains the appropriate value).
    boolean requestSigning = bucketOptions.effectiveRequestSigningEnabled();
    if (requestSigning) {
      var expireAfter = bucketOptions.effectiveUrlSigningExpire();
      // Calling `enableRequestSigning` must only happen, if `effectiveRequestSigningEnabled()` is
      // true. Making this very clear with this `if`.
      requestSigning = enableRequestSigning.test(expireAfter);
    }
    config.accept(S3_REMOTE_SIGNING_ENABLED, Boolean.toString(requestSigning));
    if (requestSigning) {
      config.accept(S3_SIGNER, "S3V4RestSigner"); // Needed for pyiceberg
    }

    // Note: 'accessDelegationPredicate' returns 'true', if the client did not send the
    // 'X-Iceberg-Access-Delegation' header (or if the header contains the appropriate value).
    if (bucketOptions.effectiveClientAssumeRoleEnabled() && canDoCredentialsVending) {
      // TODO: expectedSessionDuration() should probably be declared by the client.
      S3Credentials s3credentials =
          s3CredentialsResolver.resolveSessionCredentials(bucketOptions, storageLocations);
      config.accept(S3_ACCESS_KEY_ID, s3credentials.accessKeyId());
      config.accept(S3_SECRET_ACCESS_KEY, s3credentials.secretAccessKey());
      s3credentials.sessionToken().ifPresent(t -> config.accept(S3_SESSION_TOKEN, t));
    }

    bucketOptions.tableConfigOverrides().forEach(config);
  }

  @Override
  public void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties) {
    S3BucketOptions s3BucketOptions = s3clientSupplier.s3options().resolveOptionsForUri(warehouse);

    properties.accept(
        "iceberg.rest-catalog.vended-credentials-enabled",
        Boolean.toString(s3BucketOptions.effectiveClientAssumeRoleEnabled()));
    properties.accept("fs.native-s3.enabled", "true");
    if (icebergConfig.containsKey(S3_ENDPOINT)) {
      properties.accept("s3.endpoint", icebergConfig.get(S3_ENDPOINT));
    }
    if (icebergConfig.containsKey(S3_CLIENT_REGION)) {
      properties.accept("s3.region", icebergConfig.get(S3_CLIENT_REGION));
    }
    properties.accept(
        "s3.path-style-access", icebergConfig.getOrDefault(S3_PATH_STYLE_ACCESS, "false"));
  }

  S3BucketOptions icebergConfigOverrides(StorageUri warehouse, BiConsumer<String, String> config) {

    S3NamedBucketOptions bucketOptions =
        s3clientSupplier.s3options().resolveOptionsForUri(warehouse);
    bucketOptions.region().ifPresent(r -> config.accept(S3_CLIENT_REGION, r));
    if (bucketOptions.externalEndpoint().isPresent()) {
      config.accept(S3_ENDPOINT, bucketOptions.externalEndpoint().get().toString());
    } else {
      bucketOptions.endpoint().ifPresent(e -> config.accept(S3_ENDPOINT, e.toString()));
    }
    bucketOptions
        .accessPoint()
        .ifPresent(
            ap ->
                config.accept(
                    S3_ACCESS_POINTS_PREFIX + bucketOptions.authority().orElseThrow(), ap));
    bucketOptions
        .allowCrossRegionAccessPoint()
        .ifPresent(allow -> config.accept(S3_USE_ARN_REGION_ENABLED, allow ? "true" : "false"));
    bucketOptions
        .pathStyleAccess()
        .ifPresent(psa -> config.accept(S3_PATH_STYLE_ACCESS, psa ? "true" : "false"));

    return bucketOptions;
  }

  void icebergConfigDefaults(StorageUri warehouse, BiConsumer<String, String> config) {
    S3BucketOptions bucketOptions = s3clientSupplier.s3options().resolveOptionsForUri(warehouse);
    bucketOptions.region().ifPresent(x -> config.accept(S3_CLIENT_REGION, x));
    config.accept(PYICEBERG_FILE_IO_IMPL, "pyiceberg.io.fsspec.FsspecFileIO");
    config.accept(ICEBERG_FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
  }
}

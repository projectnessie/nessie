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
package org.projectnessie.catalog.service.rest;

import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.projectnessie.catalog.files.adls.AdlsLocation.adlsLocation;
import static org.projectnessie.catalog.files.s3.S3Utils.isS3scheme;
import static org.projectnessie.catalog.files.s3.S3Utils.normalizeS3Scheme;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.catalog.files.adls.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.adls.AdlsLocation;
import org.projectnessie.catalog.files.adls.AdlsOptions;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions;
import org.projectnessie.catalog.files.gcs.GcsOptions;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3Credentials;
import org.projectnessie.catalog.files.s3.S3CredentialsResolver;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.model.ContentKey;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.storage.uri.StorageUri;

@RequestScoped
public class IcebergConfigurer {

  static final String ICEBERG_WAREHOUSE_LOCATION = "warehouse";
  static final String ICEBERG_PREFIX = "prefix";
  static final String FILE_IO_IMPL = "io-impl";

  static final String METRICS_REPORTING_ENABLED = "rest-metrics-reporting-enabled";

  static final String S3_CLIENT_REGION = "client.region";
  static final String S3_ACCESS_KEY_ID = "s3.access-key-id";
  static final String S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  static final String S3_SESSION_TOKEN = "s3.session-token";
  static final String S3_ENDPOINT = "s3.endpoint";
  static final String S3_ACCESS_POINTS_PREFIX = "s3.access-points.";
  static final String S3_PATH_STYLE_ACCESS = "s3.path-style-access";
  static final String S3_USE_ARN_REGION_ENABLED = "s3.use-arn-region-enabled";
  static final String S3_REMOTE_SIGNING_ENABLED = "s3.remote-signing-enabled";

  /** Base URI of the signer endpoint, defaults to {@code uri}. */
  static final String S3_SIGNER_URI = "s3.signer.uri";

  /** Path of the signer endpoint. */
  static final String S3_SIGNER_ENDPOINT = "s3.signer.endpoint";

  static final String GCS_PROJECT_ID = "gcs.project-id";
  static final String GCS_CLIENT_LIB_TOKEN = "gcs.client-lib-token";
  static final String GCS_SERVICE_HOST = "gcs.service.host";
  static final String GCS_DECRYPTION_KEY = "gcs.decryption-key";
  static final String GCS_ENCRYPTION_KEY = "gcs.encryption-key";
  static final String GCS_USER_PROJECT = "gcs.user-project";
  static final String GCS_READ_CHUNK_SIZE = "gcs.channel.read.chunk-size-bytes";
  static final String GCS_WRITE_CHUNK_SIZE = "gcs.channel.write.chunk-size-bytes";
  static final String GCS_DELETE_BATCH_SIZE = "gcs.delete.batch-size";
  static final String GCS_OAUTH2_TOKEN = "gcs.oauth2.token";
  static final String GCS_OAUTH2_TOKEN_EXPIRES_AT = "gcs.oauth2.token-expires-at";
  static final String GCS_NO_AUTH = "gcs.no-auth";

  static final String ADLS_SHARED_KEY_ACCOUNT_NAME = "adls.auth.shared-key.account.name";
  static final String ADLS_SHARED_KEY_ACCOUNT_KEY = "adls.auth.shared-key.account.key";
  static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  static final String ADLS_CONNECTION_STRING_PREFIX = "adls.connection-string.";
  static final String ADLS_READ_BLOCK_SIZE_BYTES = "adls.read.block-size-bytes";
  static final String ADLS_WRITE_BLOCK_SIZE_BYTES = "adls.write.block-size-bytes";

  @Inject ServerConfig serverConfig;
  @Inject CatalogConfig catalogConfig;
  @Inject S3CredentialsResolver s3CredentialsResolver;
  @Inject S3Options<?> s3Options;
  @Inject GcsOptions<?> gcsOptions;
  @Inject AdlsOptions<?> adlsOptions;
  @Inject SecretsProvider secretsProvider;

  @Context ExternalBaseUri uriInfo;

  public Map<String, String> icebergConfigDefaults(String reference, String warehouse) {
    boolean hasWarehouse = warehouse != null && !warehouse.isEmpty();
    WarehouseConfig warehouseConfig = catalogConfig.getWarehouse(warehouse);

    String branch = defaultBranchName(reference);
    Map<String, String> config = new HashMap<>();
    // Not fully implemented yet
    config.put(METRICS_REPORTING_ENABLED, "false");
    config.put(FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
    config.put(ICEBERG_WAREHOUSE_LOCATION, warehouseConfig.location());
    config.putAll(uriInfo.icebergConfigDefaults());
    config.putAll(storeConfigDefaults(URI.create(warehouseConfig.location())));
    // allow users to override the 'rest-page-size' in the Nessie configuration
    config.put("rest-page-size", "200");
    config.putAll(catalogConfig.icebergConfigDefaults());
    config.putAll(warehouseConfig.icebergConfigDefaults());
    // Set the "default" prefix
    if (!hasWarehouse && catalogConfig.defaultWarehouse().isPresent()) {
      config.put(ICEBERG_PREFIX, encode(branch, UTF_8));
    } else {
      config.put(
          ICEBERG_PREFIX,
          encode(branch + "|" + catalogConfig.resolveWarehouseName(warehouse), UTF_8));
    }
    return config;
  }

  public Map<String, String> icebergConfigOverrides(String reference, String warehouse) {
    WarehouseConfig warehouseConfig = catalogConfig.getWarehouse(warehouse);
    String branch = defaultBranchName(reference);
    Map<String, String> config = new HashMap<>();
    config.putAll(uriInfo.icebergConfigOverrides());
    config.putAll(storeConfigOverrides(StorageUri.of(warehouseConfig.location())));
    config.putAll(catalogConfig.icebergConfigOverrides());
    config.putAll(warehouseConfig.icebergConfigOverrides());
    // Marker property telling clients that the backend is a Nessie Catalog.
    config.put("nessie.is-nessie-catalog", "true");
    // 'prefix-pattern' is just for information at the moment...
    config.put("nessie.prefix-pattern", "{ref}|{warehouse}");
    // The following properties are passed back to clients to automatically configure their Nessie
    // client. These properties are _not_ user configurable properties.
    config.put("nessie.default-branch.name", branch);
    return config;
  }

  public Map<String, String> icebergConfigPerTable(
      IcebergTableMetadata tableMetadata, String prefix, ContentKey contentKey) {
    Map<String, String> config = new HashMap<>();
    URI location = URI.create(tableMetadata.location());
    // TODO this is the place to add vended authorization tokens for file/object access
    // TODO add (correct) S3_CLIENT_REGION for the table here (based on the table's location?)
    if (isS3scheme(location.getScheme())) {
      // Must use both 's3.signer.uri' and 's3.signer.endpoint', because Iceberg before 1.5.0 does
      // not handle full URIs passed via 's3.signer.endpoint'. This was changed via
      // https://github.com/apache/iceberg/pull/8976/files#diff-1f7498b6989fffc169f7791292ed2ccb35b305f6a547fd832f6724057c8aca8bR213-R216,
      // first released in Iceberg 1.5.0. It's unclear how other language implementations deal with
      // this.
      config.put(S3_SIGNER_URI, uriInfo.icebergBaseURI().toString());
      config.put(
          S3_SIGNER_ENDPOINT,
          uriInfo.icebergS3SignerPath(
              prefix, contentKey, normalizeS3Scheme(tableMetadata.location())));
    }
    // TODO GCS and ADLS per-table config overrides
    return config;
  }

  private String defaultBranchName(String reference) {
    String branch = reference;
    if (branch == null) {
      branch = serverConfig.getDefaultBranch();
    }
    if (branch == null) {
      branch = "main";
    }
    return branch;
  }

  public Map<String, String> storeConfigDefaults(URI warehouseLocation) {
    Map<String, String> configDefaults = new HashMap<>();
    if (isS3scheme(warehouseLocation.getScheme())) {
      S3BucketOptions bucketOptions =
          s3Options.effectiveOptionsForBucket(
              Optional.of(warehouseLocation.getAuthority()), secretsProvider);
      bucketOptions.region().ifPresent(x -> configDefaults.put(S3_CLIENT_REGION, x));
    }
    return configDefaults;
  }

  public Map<String, String> storeConfigOverrides(StorageUri warehouseLocation) {
    String scheme = warehouseLocation.scheme();
    if (scheme != null) {
      switch (scheme) {
        case "s3":
        case "s3a":
        case "s3n":
          return s3ConfigOverrides(warehouseLocation);
        case "gs":
          return gcsConfigOverrides(warehouseLocation);
        case "abfs":
        case "abfss":
          return adlsConfigOverrides(warehouseLocation);
        default:
          break;
      }
    }
    return Map.of();
  }

  private Map<String, String> s3ConfigOverrides(StorageUri warehouseLocation) {
    Map<String, String> configOverrides = new HashMap<>();
    String bucket = warehouseLocation.requiredAuthority();
    S3BucketOptions s3BucketOptions =
        s3Options.effectiveOptionsForBucket(Optional.of(bucket), secretsProvider);
    s3BucketOptions.region().ifPresent(r -> configOverrides.put(S3_CLIENT_REGION, r));
    if (s3BucketOptions.externalEndpoint().isPresent()) {
      configOverrides.put(S3_ENDPOINT, s3BucketOptions.externalEndpoint().get().toString());
    } else {
      s3BucketOptions.endpoint().ifPresent(e -> configOverrides.put(S3_ENDPOINT, e.toString()));
    }
    s3BucketOptions
        .accessPoint()
        .ifPresent(ap -> configOverrides.put(S3_ACCESS_POINTS_PREFIX + bucket, ap));
    s3BucketOptions
        .allowCrossRegionAccessPoint()
        .ifPresent(
            allow -> configOverrides.put(S3_USE_ARN_REGION_ENABLED, allow ? "true" : "false"));
    s3BucketOptions
        .pathStyleAccess()
        .ifPresent(psa -> configOverrides.put(S3_PATH_STYLE_ACCESS, psa ? "true" : "false"));

    switch (s3BucketOptions.effectiveClientAuthenticationMode()) {
      case REQUEST_SIGNING:
        configOverrides.put(S3_REMOTE_SIGNING_ENABLED, "true");
        break;

      case ASSUME_ROLE:
        // TODO: expectedSessionDuration() should probably be declared by the client.
        S3Credentials s3credentials =
            s3CredentialsResolver.resolveSessionCredentials(s3BucketOptions);
        configOverrides.put(S3_ACCESS_KEY_ID, s3credentials.accessKeyId());
        configOverrides.put(S3_SECRET_ACCESS_KEY, s3credentials.secretAccessKey());
        s3credentials.sessionToken().ifPresent(t -> configOverrides.put(S3_SESSION_TOKEN, t));
        configOverrides.put(S3_REMOTE_SIGNING_ENABLED, "false");
        break;

      default:
        throw new IllegalArgumentException(
            "Unsupported client authentication mode: "
                + s3BucketOptions.clientAuthenticationMode());
    }
    return configOverrides;
  }

  private Map<String, String> gcsConfigOverrides(StorageUri warehouseLocation) {
    Map<String, String> configOverrides = new HashMap<>();
    String bucket = warehouseLocation.requiredAuthority();
    GcsBucketOptions gcsBucketOptions =
        gcsOptions.effectiveOptionsForBucket(Optional.of(bucket), secretsProvider);
    gcsBucketOptions.projectId().ifPresent(p -> configOverrides.put(GCS_PROJECT_ID, p));
    gcsBucketOptions.clientLibToken().ifPresent(t -> configOverrides.put(GCS_CLIENT_LIB_TOKEN, t));
    gcsBucketOptions.host().ifPresent(h -> configOverrides.put(GCS_SERVICE_HOST, h.toString()));
    gcsBucketOptions.userProject().ifPresent(u -> configOverrides.put(GCS_USER_PROJECT, u));
    gcsBucketOptions
        .readChunkSize()
        .ifPresent(rcs -> configOverrides.put(GCS_READ_CHUNK_SIZE, Integer.toString(rcs)));
    gcsBucketOptions
        .writeChunkSize()
        .ifPresent(wcs -> configOverrides.put(GCS_WRITE_CHUNK_SIZE, Integer.toString(wcs)));
    gcsBucketOptions
        .deleteBatchSize()
        .ifPresent(dbs -> configOverrides.put(GCS_DELETE_BATCH_SIZE, Integer.toString(dbs)));
    if (gcsBucketOptions.authType().isPresent()
        && gcsBucketOptions.authType().get() == GcsBucketOptions.GcsAuthType.NONE) {
      configOverrides.put(GCS_NO_AUTH, "true");
    }
    return configOverrides;
  }

  private Map<String, String> adlsConfigOverrides(StorageUri warehouseLocation) {
    Map<String, String> configOverrides = new HashMap<>();
    AdlsLocation location = adlsLocation(warehouseLocation);
    Optional<String> fileSystem = location.container();
    AdlsFileSystemOptions fileSystemOptions =
        adlsOptions.effectiveOptionsForFileSystem(fileSystem, secretsProvider);
    fileSystemOptions
        .endpoint()
        .ifPresent(
            e -> configOverrides.put(ADLS_CONNECTION_STRING_PREFIX + location.storageAccount(), e));
    adlsOptions
        .readBlockSize()
        .ifPresent(r -> configOverrides.put(ADLS_READ_BLOCK_SIZE_BYTES, Integer.toString(r)));
    adlsOptions
        .writeBlockSize()
        .ifPresent(s -> configOverrides.put(ADLS_WRITE_BLOCK_SIZE_BYTES, Long.toString(s)));
    return configOverrides;
  }
}

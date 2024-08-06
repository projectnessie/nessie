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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.Clock.systemUTC;
import static org.projectnessie.catalog.files.adls.AdlsLocation.adlsLocation;
import static org.projectnessie.catalog.files.s3.S3Utils.normalizeS3Scheme;
import static org.projectnessie.catalog.files.s3.StorageLocations.storageLocations;
import static org.projectnessie.catalog.service.rest.AccessDelegation.REMOTE_SIGNING;
import static org.projectnessie.catalog.service.rest.AccessDelegation.VENDED_CREDENTIALS;
import static org.projectnessie.catalog.service.rest.AccessDelegation.accessDelegationPredicate;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.catalog.files.NormalizedObjectStoreOptions;
import org.projectnessie.catalog.files.adls.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.adls.AdlsLocation;
import org.projectnessie.catalog.files.adls.AdlsOptions;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions;
import org.projectnessie.catalog.files.gcs.GcsOptions;
import org.projectnessie.catalog.files.s3.S3BucketOptions;
import org.projectnessie.catalog.files.s3.S3Credentials;
import org.projectnessie.catalog.files.s3.S3CredentialsResolver;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.files.s3.StorageLocations;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.service.api.SignerKeysService;
import org.projectnessie.catalog.service.config.CatalogConfig;
import org.projectnessie.catalog.service.config.WarehouseConfig;
import org.projectnessie.catalog.service.objtypes.SignerKey;
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
  @Inject @NormalizedObjectStoreOptions S3Options s3Options;
  @Inject @NormalizedObjectStoreOptions GcsOptions gcsOptions;
  @Inject @NormalizedObjectStoreOptions AdlsOptions adlsOptions;
  @Inject SecretsProvider secretsProvider;
  @Inject SignerKeysService signerKeysService;

  @Inject
  @ConfigProperty(name = "nessie.server.authentication.enabled")
  boolean authnEnabled;

  @Context ExternalBaseUri uriInfo;

  public Response trinoConfig(String reference, String warehouse, String format) {

    WarehouseConfig warehouseConfig = catalogConfig.getWarehouse(warehouse);

    StorageUri location = StorageUri.of(warehouseConfig.location());
    StorageLocations storageLocations =
        storageLocations(StorageUri.of(warehouseConfig.location()), List.of(location), List.of());

    Map<String, String> config = icebergConfigOverrides(reference, warehouse);
    // Exclude VENDED_CREDENTIALS to prevent an STS round trip here
    storeConfigOverrides(
        storageLocations, config, null, null, ad -> !ad.equals(VENDED_CREDENTIALS));

    Properties properties = new Properties();

    properties.put("connector.name", "iceberg");
    properties.put("iceberg.catalog.type", "rest");
    properties.put("iceberg.rest-catalog.uri", uriInfo.icebergBaseURI().toString());

    properties.put("iceberg.rest-catalog.security", authnEnabled ? "OAUTH2" : "NONE");
    if (authnEnabled) {
      properties.put(
          "iceberg.rest-catalog.oauth2.token", "fill-in-your-oauth-token or use .credential");
      properties.put(
          "iceberg.rest-catalog.oauth2.credential", "fill-in-your-oauth-credentials or use .token");
    }

    String scheme = location.scheme();
    if (scheme != null) {
      switch (scheme) {
        case "s3":
        case "s3a":
        case "s3n":
          String bucket = location.requiredAuthority();
          S3BucketOptions s3BucketOptions =
              s3Options.effectiveOptionsForBucket(Optional.of(bucket), secretsProvider);

          properties.put(
              "iceberg.rest-catalog.vended-credentials-enabled",
              Boolean.toString(s3BucketOptions.effectiveClientAssumeRoleEnabled()));
          properties.put("fs.native-s3.enabled", "true");
          if (config.containsKey(S3_ENDPOINT)) {
            properties.put("s3.endpoint", config.get(S3_ENDPOINT));
          }
          if (config.containsKey(S3_CLIENT_REGION)) {
            properties.put("s3.region", config.get(S3_CLIENT_REGION));
          }
          properties.put(
              "s3.path-style-access", config.getOrDefault(S3_PATH_STYLE_ACCESS, "false"));

          break;
        case "gs":
          gcsConfigOverrides(storageLocations, config);
          properties.put("fs.native-gcs.enabled", "true");
          properties.put("gcs.project-id", config.get(GCS_PROJECT_ID));
          if (config.containsKey(GCS_READ_CHUNK_SIZE)) {
            properties.put("gcs.read-block-size", config.get(GCS_READ_CHUNK_SIZE));
          }
          if (config.containsKey(GCS_WRITE_CHUNK_SIZE)) {
            properties.put("gcs.write-block-size", config.get(GCS_WRITE_CHUNK_SIZE));
          }
          break;
        case "abfs":
        case "abfss":
          adlsConfigOverrides(storageLocations, config);
          properties.put("fs.native-azure.enabled", "true");
          if (config.containsKey(ADLS_READ_BLOCK_SIZE_BYTES)) {
            properties.put("azure.read-block-size", config.get(ADLS_READ_BLOCK_SIZE_BYTES));
          }
          if (config.containsKey(ADLS_WRITE_BLOCK_SIZE_BYTES)) {
            properties.put("azure.write-block-size", config.get(ADLS_WRITE_BLOCK_SIZE_BYTES));
          }
          break;
        default:
          break;
      }
    }

    List<String> header =
        List.of(
            "Example Trino starter configuration properties for warehouse " + location,
            "generated by Nessie to be placed for example in",
            "/etc/trino/catalogs/nessie.properties within a Trino container/pod when.",
            "using Trino 'static' configurations.",
            "",
            "This starter configuration must be inspected and verified to validate that",
            "all options and values match your specific needs and no mandatory options",
            "are missing or superfluous options are present.",
            "",
            "When using OAuth2, you have to supply the 'iceberg.rest-catalog.oauth2.token'",
            "configuration.",
            "",
            "WARNING! Trino lacks functionality to configure the oauth endpoint and is therefore",
            "unable to work with any Iceberg REST catalog implementation and demands a standard",
            "OAuth2 server like Keycloak or Authelia. If you feel you need client-ID/secret flow,",
            "please report an issue against Trino.",
            "",
            "No guarantees that this configuration works for your specific needs.",
            "Use at your own risk!",
            "Do not distribute the contents as those may contain sensitive information!");
    format = format == null ? "properties" : format.toLowerCase(Locale.ROOT).trim();
    switch (format) {
      case "sql":
      case "ddl":
      case "dynamic":
        String sql =
            properties.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().toString()))
                .map(e -> format("    \"%s\" = '%s'", e.getKey(), e.getValue()))
                .collect(
                    Collectors.joining(
                        ",\n", "CREATE CATALOG nessie\n  USING iceberg\n  WITH (\n", "\n  );\n"));

        return Response.ok(
                header.stream().collect(Collectors.joining("\n * ", "/*\n * ", "\n */\n")) + sql,
                "application/sql")
            .header("Content-Disposition", "attachment; filename=\"create-catalog-nessie.sql\"")
            .build();
      case "properties":
      case "static":
      default:
        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
          try {
            pw.println("#\n# " + String.join("\n# ", header) + "\n#\n");
            properties.store(pw, "");
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return Response.ok(sw.toString(), "text/plain")
            .header("Content-Disposition", "attachment; filename=\"nessie.properties\"")
            .build();
    }
  }

  public Map<String, String> icebergConfigDefaults(String reference, String warehouse) {
    boolean hasWarehouse = warehouse != null && !warehouse.isEmpty();
    WarehouseConfig warehouseConfig = catalogConfig.getWarehouse(warehouse);

    String branch = defaultBranchName(reference);
    Map<String, String> config = new HashMap<>();
    // Not fully implemented yet
    config.put(METRICS_REPORTING_ENABLED, "false");
    config.put(ICEBERG_WAREHOUSE_LOCATION, warehouseConfig.location());
    config.putAll(uriInfo.icebergConfigDefaults());
    storeConfigDefaults(StorageUri.of(warehouseConfig.location()), config);
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
    Map<String, String> config = new HashMap<>(uriInfo.icebergConfigOverrides());
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

  IcebergTableConfig icebergConfigPerTable(
      NessieEntitySnapshot<?> nessieSnapshot,
      String warehouseLocation,
      IcebergTableMetadata tableMetadata,
      String prefix,
      ContentKey contentKey,
      String dataAccess,
      boolean writeAccessGranted) {
    ImmutableIcebergTableConfig.Builder tableConfig = ImmutableIcebergTableConfig.builder();

    Set<StorageUri> writeable = new HashSet<>();
    Set<StorageUri> readOnly = new HashSet<>();
    Set<StorageUri> maybeWriteable = writeAccessGranted ? writeable : readOnly;
    StorageUri locationUri = StorageUri.of(tableMetadata.location());
    (tableMetadata.location().startsWith(warehouseLocation) ? maybeWriteable : readOnly)
        .add(locationUri);

    if (!icebergWriteObjectStorage(tableConfig, tableMetadata.properties(), warehouseLocation)) {
      String writeLocation = icebergWriteLocation(tableMetadata.properties());
      if (writeLocation != null && !writeLocation.startsWith(tableMetadata.location())) {
        (writeLocation.startsWith(warehouseLocation) ? maybeWriteable : readOnly)
            .add(StorageUri.of(writeLocation));
      }
    }

    for (String additionalKnownLocation : nessieSnapshot.additionalKnownLocations()) {
      StorageUri old = StorageUri.of(additionalKnownLocation);
      if (!writeable.contains(old)) {
        readOnly.add(old);
      }
    }

    StorageLocations storageLocations =
        storageLocations(StorageUri.of(warehouseLocation), writeable, readOnly);

    Predicate<AccessDelegation> accessDelegationPredicate = accessDelegationPredicate(dataAccess);

    Map<String, String> config = new HashMap<>();

    storeConfigDefaults(locationUri, config);

    storeConfigOverrides(storageLocations, config, prefix, contentKey, accessDelegationPredicate);

    return tableConfig.config(config).build();
  }

  static boolean icebergWriteObjectStorage(
      ImmutableIcebergTableConfig.Builder config,
      Map<String, String> metadataProperties,
      String bucketLocation) {
    if (!Boolean.parseBoolean(
        metadataProperties.getOrDefault("write.object-storage.enabled", "false"))) {
      return false;
    }

    Map<String, String> updated = new HashMap<>(metadataProperties);

    updated.put("write.data.path", bucketLocation);
    updated.remove("write.object-storage.path");
    updated.remove("write.folder-storage.path");

    config.updatedMetadataProperties(updated);

    return true;
  }

  static String icebergWriteLocation(Map<String, String> properties) {
    String dataLocation = properties.get("write.data.path");
    if (dataLocation == null) {
      dataLocation = properties.get("write.object-storage.path");
      if (dataLocation == null) {
        dataLocation = properties.get("write.folder-storage.path");
      }
    }
    return dataLocation;
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

  public void storeConfigDefaults(StorageUri location, Map<String, String> config) {
    String scheme = location.scheme();
    if (scheme != null) {
      switch (scheme) {
        case "s3":
        case "s3a":
        case "s3n":
          S3BucketOptions bucketOptions =
              s3Options.effectiveOptionsForBucket(
                  Optional.ofNullable(location.authority()), secretsProvider);
          bucketOptions.region().ifPresent(x -> config.put(S3_CLIENT_REGION, x));
          config.put(FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
          return;
        case "gs":
          config.put(FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");
          return;
        case "abfs":
        case "abfss":
          config.put(FILE_IO_IMPL, "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
          return;
        default:
          config.put(FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO");
          break;
      }
    }
  }

  public void storeConfigOverrides(
      StorageLocations storageLocations,
      Map<String, String> config,
      String prefix,
      ContentKey contentKey,
      Predicate<AccessDelegation> accessDelegationPredicate) {
    Set<String> schemes =
        Stream.concat(
                storageLocations.writeableLocations().stream(),
                storageLocations.readonlyLocations().stream())
            .map(StorageUri::scheme)
            .collect(Collectors.toSet());

    checkArgument(
        schemes.size() == 1,
        "Only one scheme allowed, but access to '%s' includes schemes %s",
        schemes);

    String scheme = schemes.iterator().next();
    if (scheme != null) {
      switch (scheme) {
        case "s3":
        case "s3a":
        case "s3n":
          s3ConfigOverrides(
              storageLocations, config, prefix, contentKey, accessDelegationPredicate);
          return;
        case "gs":
          gcsConfigOverrides(storageLocations, config);
          return;
        case "abfs":
        case "abfss":
          adlsConfigOverrides(storageLocations, config);
          return;
        default:
          break;
      }
    }
  }

  private void s3ConfigOverrides(
      StorageLocations storageLocations,
      Map<String, String> configOverrides,
      String prefix,
      ContentKey contentKey,
      Predicate<AccessDelegation> accessDelegationPredicate) {

    Set<String> buckets =
        Stream.concat(
                storageLocations.writeableLocations().stream(),
                storageLocations.readonlyLocations().stream())
            .map(StorageUri::requiredAuthority)
            .collect(Collectors.toSet());

    checkState(buckets.size() == 1, "Only one S3 bucket supported");
    String bucket = buckets.iterator().next();

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

    // Note: 'accessDelegationPredicate' returns 'true', if the client did not send the
    // 'X-Iceberg-Access-Delegation' header (or if the header contains the appropriate value).
    if (s3BucketOptions.effectiveRequestSigningEnabled()
        && accessDelegationPredicate.test(REMOTE_SIGNING)) {
      configOverrides.put(S3_REMOTE_SIGNING_ENABLED, "true");
      if (prefix != null && contentKey != null) {
        // Must use both 's3.signer.uri' and 's3.signer.endpoint', because Iceberg before 1.5.0 does
        // not handle full URIs passed via 's3.signer.endpoint'. This was changed via
        // https://github.com/apache/iceberg/pull/8976/files#diff-1f7498b6989fffc169f7791292ed2ccb35b305f6a547fd832f6724057c8aca8bR213-R216,
        // first released in Iceberg 1.5.0. It's unclear how other language implementations deal
        // with this.
        configOverrides.put(S3_SIGNER_URI, uriInfo.icebergBaseURI().toString());

        String warehouseLocation =
            normalizeS3Scheme(storageLocations.warehouseLocation().toString());

        List<String> normalizedWriteLocations = new ArrayList<>();
        List<String> normalizedReadLocations = new ArrayList<>();
        for (StorageUri loc : storageLocations.writeableLocations()) {
          String locStr = normalizeS3Scheme(loc.toString());
          if (locStr.startsWith(warehouseLocation)) {
            normalizedWriteLocations.add(locStr);
          }
        }
        for (StorageUri loc : storageLocations.readonlyLocations()) {
          String locStr = normalizeS3Scheme(loc.toString());
          normalizedReadLocations.add(locStr);
        }

        SignerKey signerKey = signerKeysService.currentSignerKey();

        long expirationTimestamp = systemUTC().instant().plus(3, ChronoUnit.HOURS).getEpochSecond();

        String contentKeyPathString = contentKey.toPathString();
        String uriQuery =
            SignerSignature.builder()
                .expirationTimestamp(expirationTimestamp)
                .prefix(prefix)
                .identifier(contentKeyPathString)
                .warehouseLocation(warehouseLocation)
                .writeLocations(normalizedWriteLocations)
                .readLocations(normalizedReadLocations)
                .build()
                .uriQuery(signerKey);

        configOverrides.put(
            S3_SIGNER_ENDPOINT,
            uriInfo.icebergS3SignerPath(prefix, contentKeyPathString, uriQuery));
      }
    } else {
      configOverrides.put(S3_REMOTE_SIGNING_ENABLED, "false");
    }

    // Note: 'accessDelegationPredicate' returns 'true', if the client did not send the
    // 'X-Iceberg-Access-Delegation' header (or if the header contains the appropriate value).
    if (s3BucketOptions.effectiveClientAssumeRoleEnabled()
        && accessDelegationPredicate.test(VENDED_CREDENTIALS)) {
      // TODO: expectedSessionDuration() should probably be declared by the client.
      S3Credentials s3credentials =
          s3CredentialsResolver.resolveSessionCredentials(s3BucketOptions, storageLocations);
      configOverrides.put(S3_ACCESS_KEY_ID, s3credentials.accessKeyId());
      configOverrides.put(S3_SECRET_ACCESS_KEY, s3credentials.secretAccessKey());
      s3credentials.sessionToken().ifPresent(t -> configOverrides.put(S3_SESSION_TOKEN, t));
    }
  }

  private void gcsConfigOverrides(
      StorageLocations storageLocations, Map<String, String> configOverrides) {

    Set<String> buckets =
        Stream.concat(
                storageLocations.writeableLocations().stream(),
                storageLocations.readonlyLocations().stream())
            .map(StorageUri::requiredAuthority)
            .collect(Collectors.toSet());

    checkState(buckets.size() == 1, "Only one GCS bucket supported");
    String bucket = buckets.iterator().next();

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
    if (gcsBucketOptions.effectiveAuthType() == GcsBucketOptions.GcsAuthType.NONE) {
      configOverrides.put(GCS_NO_AUTH, "true");
    }
  }

  private void adlsConfigOverrides(
      StorageLocations storageLocations, Map<String, String> configOverrides) {
    Set<StorageUri> storageUris =
        Stream.concat(
                storageLocations.writeableLocations().stream(),
                storageLocations.readonlyLocations().stream())
            .collect(Collectors.toSet());

    checkState(storageUris.size() == 1, "Only one ADLS location supported");
    StorageUri storageUri = storageUris.iterator().next();

    AdlsLocation location = adlsLocation(storageUri);
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
  }
}

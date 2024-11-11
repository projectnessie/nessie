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

import static java.lang.String.format;
import static java.util.regex.Pattern.quote;
import static org.projectnessie.catalog.files.gcs.GcsClients.buildCredentials;

import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.Storage;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsConfig;
import org.projectnessie.catalog.files.config.GcsDownscopedCredentials;
import org.projectnessie.catalog.files.config.GcsOptions;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GcsStorageSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(GcsStorageSupplier.class);

  // Suitable for both old object-storage layout (before Iceberg 1.7.0) and new (since 1.7.0)
  static final String RANDOMIZED_PART = "([A-Za-z0-9=]+/|[01]{4}/[01]{4}/[01]{4}/[01]{8}/)?";

  private final HttpTransportFactory httpTransportFactory;
  private final GcsConfig gcsConfig;
  private final GcsOptions gcsOptions;
  private final SecretsProvider secretsProvider;

  public GcsStorageSupplier(
      HttpTransportFactory httpTransportFactory,
      GcsConfig gcsConfig,
      GcsOptions gcsOptions,
      SecretsProvider secretsProvider) {
    this.httpTransportFactory = httpTransportFactory;
    this.gcsConfig = gcsConfig;
    this.gcsOptions = gcsOptions;
    this.secretsProvider = secretsProvider;
  }

  GcsOptions gcsOptions() {
    return gcsOptions;
  }

  SecretsProvider secretsProvider() {
    return secretsProvider;
  }

  public GcsBucketOptions bucketOptions(StorageUri location) {
    return gcsOptions.resolveOptionsForUri(location);
  }

  public Storage forLocation(GcsBucketOptions bucketOptions) {
    return GcsClients.buildStorage(gcsConfig, bucketOptions, httpTransportFactory, secretsProvider);
  }

  public Optional<TokenSecret> generateDelegationToken(
      StorageLocations storageLocations, GcsBucketOptions gcsBucketOptions) {

    if (!gcsBucketOptions
        .downscopedCredentials()
        .flatMap(GcsDownscopedCredentials::enable)
        .orElse(false)) {
      return Optional.empty();
    }

    DownscopedCredentials.Builder downscopedCredentialsBuilder = DownscopedCredentials.newBuilder();

    Credentials bucketCredentials =
        buildCredentials(gcsBucketOptions, httpTransportFactory, secretsProvider);
    if (bucketCredentials instanceof GoogleCredentials) {
      GoogleCredentials sourceCredentials = (GoogleCredentials) bucketCredentials;
      downscopedCredentialsBuilder.setSourceCredential(sourceCredentials);
    } else if (bucketCredentials instanceof OAuth2Credentials) {
      // OAuth2Credentials does not extend GoogleCredentials
      downscopedCredentialsBuilder.setAccessToken(
          ((OAuth2Credentials) bucketCredentials).getAccessToken());
    } else {
      LOGGER.warn(
          "No suitable credentials to generate a downscoped token to access warehouse {}",
          storageLocations.warehouseLocation());
      return Optional.empty();
    }

    CredentialAccessBoundary accessBoundary = generateAccessBoundaryRules(storageLocations);

    downscopedCredentialsBuilder
        .setHttpTransportFactory(httpTransportFactory)
        .setCredentialAccessBoundary(accessBoundary);

    gcsBucketOptions
        .downscopedCredentials()
        .flatMap(GcsDownscopedCredentials::expirationMargin)
        .ifPresent(downscopedCredentialsBuilder::setExpirationMargin);
    gcsBucketOptions
        .downscopedCredentials()
        .flatMap(GcsDownscopedCredentials::refreshMargin)
        .ifPresent(downscopedCredentialsBuilder::setRefreshMargin);

    DownscopedCredentials downscopedCredentials = downscopedCredentialsBuilder.build();

    try {
      AccessToken token = downscopedCredentials.refreshAccessToken();
      return Optional.of(
          TokenSecret.tokenSecret(
              token.getTokenValue(), Instant.ofEpochMilli(token.getExpirationTime().getTime())));
    } catch (IOException e) {
      LOGGER.error(
          "Failed refresh access token for downscoped credentials for warehouse {}",
          storageLocations.warehouseLocation(),
          e);
      throw new RuntimeException("Unable to fetch access credentials " + e.getMessage());
    }
  }

  @VisibleForTesting
  public static CredentialAccessBoundary generateAccessBoundaryRules(
      StorageLocations storageLocations) {
    Map<String, List<String>> readConditionsMap = new HashMap<>();
    Map<String, List<String>> writeConditionsMap = new HashMap<>();

    Set<String> readBuckets = new HashSet<>();
    Set<String> writeBuckets = new HashSet<>();
    Stream.concat(
            storageLocations.readonlyLocations().stream(),
            storageLocations.writeableLocations().stream())
        .distinct()
        .forEach(
            location -> {
              String bucket = location.requiredAuthority();
              readBuckets.add(bucket);
              String path = location.pathWithoutLeadingTrailingSlash();
              List<String> resourceExpressions =
                  readConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());

              resourceExpressions.add(resourceNameBucketPathExpression(bucket, path));

              // list
              String listPathExpression =
                  format(
                      "api.getAttribute('storage.googleapis.com/objectListPrefix', '').matches('"
                          + RANDOMIZED_PART
                          + "%s.*')",
                      quoteForCelString(path));
              resourceExpressions.add(listPathExpression);

              if (storageLocations.writeableLocations().contains(location)) {
                writeBuckets.add(bucket);
                List<String> writeExpressions =
                    writeConditionsMap.computeIfAbsent(bucket, key -> new ArrayList<>());
                writeExpressions.add(resourceNameBucketPathExpression(bucket, path));
              }
            });

    CredentialAccessBoundary.Builder accessBoundaryBuilder = CredentialAccessBoundary.newBuilder();
    readBuckets.stream()
        .map(b -> accessBoundaryRuleForBucket(readConditionsMap, b))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(
            builder ->
                builder
                    .setAvailablePermissions(List.of("inRole:roles/storage.legacyObjectReader"))
                    // list
                    .addAvailablePermission("inRole:roles/storage.objectViewer"))
        .map(CredentialAccessBoundary.AccessBoundaryRule.Builder::build)
        .forEach(accessBoundaryBuilder::addRule);
    writeBuckets.stream()
        .map(b -> accessBoundaryRuleForBucket(writeConditionsMap, b))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(
            builder ->
                builder.setAvailablePermissions(List.of("inRole:roles/storage.legacyBucketWriter")))
        .map(CredentialAccessBoundary.AccessBoundaryRule.Builder::build)
        .forEach(accessBoundaryBuilder::addRule);
    return accessBoundaryBuilder.build();
  }

  static String resourceNameBucketPathExpression(String bucket, String path) {
    return "resource.name.matches('"
        + quoteForCelString(format("projects/_/buckets/%s/objects/", bucket))
        + RANDOMIZED_PART
        + quoteForCelString(path)
        + ".*')";
  }

  private static String quoteForCelString(String str) {
    String quoted = quote(str);
    StringBuilder sb = new StringBuilder(quoted.length() * 3 / 2);

    int l = quoted.length();
    for (int i = 0; i < l; i++) {
      char c = quoted.charAt(i);
      switch (c) {
        case '\'':
          sb.append("\\'");
          break;
        case '\"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        default:
          sb.append(c);
          break;
      }
    }

    return sb.toString();
  }

  private static Optional<CredentialAccessBoundary.AccessBoundaryRule.Builder>
      accessBoundaryRuleForBucket(Map<String, List<String>> conditionsMap, String bucket) {
    List<String> conditions = conditionsMap.get(bucket);
    if (conditions == null || conditions.isEmpty()) {
      return Optional.empty();
    }

    CredentialAccessBoundary.AccessBoundaryRule.Builder builder =
        CredentialAccessBoundary.AccessBoundaryRule.newBuilder();
    builder.setAvailableResource(bucketResource(bucket));
    builder.setAvailabilityCondition(
        AvailabilityCondition.newBuilder().setExpression(String.join(" || ", conditions)).build());

    return Optional.of(builder);
  }

  private static String bucketResource(String bucket) {
    return "//storage.googleapis.com/projects/_/buckets/" + bucket;
  }
}

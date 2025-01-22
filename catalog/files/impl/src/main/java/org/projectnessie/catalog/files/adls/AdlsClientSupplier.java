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
package org.projectnessie.catalog.files.adls;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.projectnessie.catalog.files.adls.AdlsLocation.adlsLocation;
import static org.projectnessie.catalog.files.config.AdlsFileSystemOptions.DELEGATION_KEY_DEFAULT_EXPIRY;
import static org.projectnessie.catalog.files.config.AdlsFileSystemOptions.DELEGATION_SAS_DEFAULT_EXPIRY;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.util.Configuration;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.azure.storage.file.datalake.models.UserDelegationKey;
import com.azure.storage.file.datalake.sas.DataLakeServiceSasSignatureValues;
import com.azure.storage.file.datalake.sas.PathSasPermission;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.function.Consumer;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.AdlsConfig;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions.AzureAuthType;
import org.projectnessie.catalog.files.config.AdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.storage.uri.StorageUri;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AdlsClientSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdlsClientSupplier.class);

  private final HttpClient httpClient;
  private final AdlsConfig adlsConfig;
  private final AdlsOptions adlsOptions;
  private final SecretsProvider secretsProvider;

  public AdlsClientSupplier(
      HttpClient httpClient,
      AdlsConfig adlsConfig,
      AdlsOptions adlsOptions,
      SecretsProvider secretsProvider) {
    this.httpClient = httpClient;
    this.adlsConfig = adlsConfig;
    this.adlsOptions = adlsOptions;
    this.secretsProvider = secretsProvider;
  }

  AdlsOptions adlsOptions() {
    return adlsOptions;
  }

  public DataLakeFileClient fileClientForLocation(StorageUri uri) {
    DataLakeFileSystemClient fileSystem = fileSystemClient(uri);
    String path = uri.requiredPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    return fileSystem.getFileClient(path);
  }

  DataLakeFileSystemClient fileSystemClient(StorageUri uri) {
    Configuration clientConfig = buildClientConfiguration();

    AdlsNamedFileSystemOptions fileSystemOptions = adlsOptions.resolveOptionsForUri(uri);

    String endpoint = endpointForLocation(uri, fileSystemOptions);

    return buildFileSystemClient(uri, fileSystemOptions, clientConfig, endpoint);
  }

  public Optional<String> generateUserDelegationSas(
      StorageLocations storageLocations, AdlsNamedFileSystemOptions fileSystemOptions) {
    if (!fileSystemOptions.effectiveUserDelegation().enable().orElse(false)) {
      return Optional.empty();
    }

    if (fileSystemOptions.authType().orElse(AzureAuthType.NONE) == AzureAuthType.NONE) {
      LOGGER.warn(
          "User delegation enabled for {}, but auth-type is NONE",
          storageLocations.warehouseLocation());
    }

    Configuration clientConfig = buildClientConfiguration();

    String endpoint = endpointForLocation(storageLocations.warehouseLocation(), fileSystemOptions);

    DataLakeFileSystemClient client =
        buildFileSystemClient(
            storageLocations.warehouseLocation(), fileSystemOptions, clientConfig, endpoint);

    DataLakeServiceClientBuilder dataLakeServiceClientBuilder =
        new DataLakeServiceClientBuilder()
            .endpoint(endpoint)
            .httpClient(httpClient)
            .configuration(clientConfig);

    checkState(
        applyCredentials(
            fileSystemOptions,
            dataLakeServiceClientBuilder::credential,
            dataLakeServiceClientBuilder::sasToken,
            dataLakeServiceClientBuilder::credential));

    Duration keyValidity =
        fileSystemOptions
            .effectiveUserDelegation()
            .keyExpiry()
            .orElse(DELEGATION_KEY_DEFAULT_EXPIRY);
    Duration sasValidity =
        fileSystemOptions
            .effectiveUserDelegation()
            .sasExpiry()
            .orElse(DELEGATION_SAS_DEFAULT_EXPIRY);

    Instant start = Instant.now();
    OffsetDateTime startTime = start.truncatedTo(ChronoUnit.SECONDS).atOffset(ZoneOffset.UTC);
    OffsetDateTime keyExpiry = start.plus(keyValidity).atOffset(ZoneOffset.UTC);
    OffsetDateTime sasExpiry = start.plus(sasValidity).atOffset(ZoneOffset.UTC);

    DataLakeServiceClient dataLakeServiceClient = dataLakeServiceClientBuilder.buildClient();
    UserDelegationKey userDelegationKey =
        dataLakeServiceClient.getUserDelegationKey(startTime, keyExpiry);

    PathSasPermission pathSasPermission = new PathSasPermission();
    pathSasPermission.setListPermission(true);
    pathSasPermission.setReadPermission(true);
    if (!storageLocations.writeableLocations().isEmpty()) {
      pathSasPermission.setAddPermission(true);
      pathSasPermission.setWritePermission(true);
      pathSasPermission.setDeletePermission(true);
    }

    DataLakeServiceSasSignatureValues sasSignatureValues =
        new DataLakeServiceSasSignatureValues(sasExpiry, pathSasPermission);

    String sasToken = client.generateUserDelegationSas(sasSignatureValues, userDelegationKey);

    return Optional.of(sasToken);
  }

  private DataLakeFileSystemClient buildFileSystemClient(
      StorageUri uri,
      AdlsNamedFileSystemOptions fileSystemOptions,
      Configuration clientConfig,
      String endpoint) {
    // MUST set the endpoint FIRST, because it ALSO sets accountName, fileSystemName and sasToken!
    // See com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder.endpoint

    DataLakeFileSystemClientBuilder clientBuilder =
        new DataLakeFileSystemClientBuilder()
            .httpClient(httpClient)
            .configuration(clientConfig)
            .endpoint(endpoint);

    buildRequestRetryOptions(fileSystemOptions).ifPresent(clientBuilder::retryOptions);
    AdlsLocation location = adlsLocation(uri);
    location.container().ifPresent(clientBuilder::fileSystemName);

    if (!applyCredentials(
        fileSystemOptions,
        clientBuilder::credential,
        clientBuilder::sasToken,
        clientBuilder::credential)) {
      clientBuilder.setAnonymousAccess();
    }

    return clientBuilder.buildClient();
  }

  private static String endpointForLocation(
      StorageUri uri, AdlsFileSystemOptions fileSystemOptions) {
    return fileSystemOptions
        .endpoint()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    format(
                        "Mandatory ADLS endpoint is not configured for storage account %s.",
                        uri.requiredAuthority())));
  }

  private Configuration buildClientConfiguration() {
    ConfigurationBuilder clientConfigBuilder = new ConfigurationBuilder();
    adlsConfig.configuration().forEach(clientConfigBuilder::putProperty);
    return clientConfigBuilder.build();
  }

  private boolean applyCredentials(
      AdlsFileSystemOptions fileSystemOptions,
      Consumer<StorageSharedKeyCredential> sharedKeyCredentialConsumer,
      Consumer<String> sasTokenConsumer,
      Consumer<TokenCredential> tokenCredentialConsumer) {
    AzureAuthType authType = fileSystemOptions.authType().orElse(AzureAuthType.NONE);
    switch (authType) {
      case NONE:
        return false;
      case STORAGE_SHARED_KEY:
        BasicCredentials account =
            fileSystemOptions
                .account()
                .map(
                    secretName ->
                        secretsProvider.getSecret(
                            secretName, SecretType.BASIC, BasicCredentials.class))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .orElseThrow(() -> new IllegalStateException("storage shared key missing"));

        sharedKeyCredentialConsumer.accept(
            new StorageSharedKeyCredential(account.name(), account.secret()));
        return true;
      case SAS_TOKEN:
        sasTokenConsumer.accept(
            fileSystemOptions
                .sasToken()
                .map(
                    secretName ->
                        secretsProvider.getSecret(secretName, SecretType.KEY, KeySecret.class))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .orElseThrow(() -> new IllegalStateException("SAS token missing"))
                .key());
        return true;
      case APPLICATION_DEFAULT:
        tokenCredentialConsumer.accept(DefaultAzureCredentialsLazy.DEFAULT_AZURE_CREDENTIAL);
        return true;
      default:
        throw new IllegalArgumentException("Unsupported auth type " + authType);
    }
  }

  private static final class DefaultAzureCredentialsLazy {
    static final DefaultAzureCredential DEFAULT_AZURE_CREDENTIAL =
        new DefaultAzureCredentialBuilder().build();
  }

  static Optional<RequestRetryOptions> buildRequestRetryOptions(
      AdlsFileSystemOptions fileSystemOptions) {
    return fileSystemOptions
        .retryPolicy()
        .flatMap(
            strategy -> {
              switch (strategy) {
                case NONE:
                  return Optional.empty();
                case EXPONENTIAL_BACKOFF:
                  return Optional.of(
                      new RequestRetryOptions(
                          RetryPolicyType.EXPONENTIAL,
                          fileSystemOptions.maxRetries().orElse(null),
                          fileSystemOptions.tryTimeout().orElse(null),
                          fileSystemOptions.retryDelay().orElse(null),
                          fileSystemOptions.maxRetryDelay().orElse(null),
                          null));
                case FIXED_DELAY:
                  return Optional.of(
                      new RequestRetryOptions(
                          RetryPolicyType.FIXED,
                          fileSystemOptions.maxRetries().orElse(null),
                          fileSystemOptions.tryTimeout().orElse(null),
                          fileSystemOptions.retryDelay().orElse(null),
                          fileSystemOptions.maxRetryDelay().orElse(null),
                          null));
                default:
                  throw new IllegalArgumentException("Invalid retry strategy: " + strategy);
              }
            });
  }
}

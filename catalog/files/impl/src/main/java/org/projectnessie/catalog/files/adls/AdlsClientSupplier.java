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

import static org.projectnessie.catalog.files.adls.AdlsLocation.adlsLocation;

import com.azure.core.http.HttpClient;
import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.FixedDelayOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.net.URI;
import java.util.Optional;
import org.projectnessie.catalog.files.secrets.SecretsProvider;

public final class AdlsClientSupplier {
  private final HttpClient httpClient;
  private final AdlsOptions<?> adlsOptions;
  private final SecretsProvider secretsProvider;

  public AdlsClientSupplier(
      HttpClient httpClient, AdlsOptions<?> adlsOptions, SecretsProvider secretsProvider) {
    this.httpClient = httpClient;
    this.adlsOptions = adlsOptions;
    this.secretsProvider = secretsProvider;
  }

  public AdlsOptions<?> adlsOptions() {
    return adlsOptions;
  }

  public DataLakeFileClient fileClientForLocation(URI uri) {
    AdlsLocation location = adlsLocation(uri);

    DataLakeFileSystemClient fileSystem = fileSystemClient(location);
    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    return fileSystem.getFileClient(path);
  }

  private DataLakeFileSystemClient fileSystemClient(AdlsLocation location) {
    ConfigurationBuilder clientConfig = new ConfigurationBuilder();
    adlsOptions.configurationOptions().forEach(clientConfig::putProperty);

    AdlsFileSystemOptions fileSystemOptions =
        adlsOptions.effectiveOptionsForFileSystem(location.container());

    DataLakeFileSystemClientBuilder clientBuilder =
        new DataLakeFileSystemClientBuilder()
            .httpClient(httpClient)
            .configuration(clientConfig.build());

    // MUST set the endpoint FIRST, because it ALSO sets accountName, fileSystemName and sasToken!
    // See com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder.endpoint

    String accountName =
        fileSystemOptions
            .accountNameRef()
            .map(secretsProvider::getSecret)
            .orElse(location.storageAccount());

    clientBuilder.endpoint(
        fileSystemOptions.endpoint().orElse(location.getUri().resolve("/").toString()));

    if (fileSystemOptions.sasTokenRef().isPresent()) {
      clientBuilder.sasToken(secretsProvider.getSecret(fileSystemOptions.sasTokenRef().get()));
    } else if (fileSystemOptions.accountKeyRef().isPresent()) {
      String accountKey = secretsProvider.getSecret(fileSystemOptions.accountKeyRef().get());
      clientBuilder.credential(new StorageSharedKeyCredential(accountName, accountKey));
    } else {
      throw new IllegalStateException(
          "Neither an SAS token nor account name and key are available for ADLS file system '"
              + location.path()
              + "'");
    }

    buildRetryOptions(fileSystemOptions).ifPresent(clientBuilder::retryOptions);
    buildRequestRetryOptions(fileSystemOptions).ifPresent(clientBuilder::retryOptions);
    location.container().ifPresent(clientBuilder::fileSystemName);

    return clientBuilder.buildClient();
  }

  // Both RetryOptions + RequestRetryOptions look redundant, but neither type inherits the other -
  // so :shrug:

  static Optional<RetryOptions> buildRetryOptions(AdlsFileSystemOptions fileSystemOptions) {
    return fileSystemOptions
        .retryPolicy()
        .flatMap(
            strategy -> {
              switch (strategy) {
                case NONE:
                  return Optional.empty();
                case EXPONENTIAL_BACKOFF:
                  ExponentialBackoffOptions exponentialBackoffOptions =
                      new ExponentialBackoffOptions();
                  fileSystemOptions.retryDelay().ifPresent(exponentialBackoffOptions::setBaseDelay);
                  fileSystemOptions
                      .maxRetryDelay()
                      .ifPresent(exponentialBackoffOptions::setMaxDelay);
                  fileSystemOptions
                      .maxRetries()
                      .ifPresent(exponentialBackoffOptions::setMaxRetries);
                  return Optional.of(new RetryOptions(exponentialBackoffOptions));
                case FIXED_DELAY:
                  FixedDelayOptions fixedDelayOptions =
                      new FixedDelayOptions(
                          fileSystemOptions.maxRetries().orElseThrow(),
                          fileSystemOptions.retryDelay().orElseThrow());
                  return Optional.of(new RetryOptions(fixedDelayOptions));
                default:
                  throw new IllegalArgumentException("Invalid retry strategy: " + strategy);
              }
            });
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

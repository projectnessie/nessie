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

import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.options.DataLakeFileInputStreamOptions;
import com.azure.storage.file.datalake.options.DataLakeFileOutputStreamOptions;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.AdlsNamedFileSystemOptions;
import org.projectnessie.catalog.files.config.AdlsOptions;
import org.projectnessie.storage.uri.StorageUri;

public class AdlsObjectIO implements ObjectIO {
  static final String ADLS_SAS_TOKEN_PREFIX = "adls.sas-token.";
  static final String ADLS_CONNECTION_STRING_PREFIX = "adls.connection-string.";
  static final String ADLS_READ_BLOCK_SIZE_BYTES = "adls.read.block-size-bytes";
  static final String ADLS_WRITE_BLOCK_SIZE_BYTES = "adls.write.block-size-bytes";

  private final AdlsClientSupplier clientSupplier;

  public AdlsObjectIO(AdlsClientSupplier clientSupplier) {
    this.clientSupplier = clientSupplier;
  }

  @Override
  public void ping(StorageUri uri) {
    DataLakeFileSystemClient fileSystem = clientSupplier.fileSystemClient(uri);
    fileSystem.getProperties();
  }

  @Override
  public InputStream readObject(StorageUri uri) throws IOException {
    DataLakeFileClient file = clientSupplier.fileClientForLocation(uri);
    DataLakeFileInputStreamOptions options = new DataLakeFileInputStreamOptions();
    clientSupplier.adlsOptions().readBlockSize().ifPresent(options::setBlockSize);
    return file.openInputStream(options).getInputStream();
  }

  @Override
  public OutputStream writeObject(StorageUri uri) {
    DataLakeFileClient file = clientSupplier.fileClientForLocation(uri);
    DataLakeFileOutputStreamOptions options = new DataLakeFileOutputStreamOptions();
    ParallelTransferOptions transferOptions = new ParallelTransferOptions();
    clientSupplier.adlsOptions().writeBlockSize().ifPresent(transferOptions::setBlockSizeLong);
    options.setParallelTransferOptions(transferOptions);
    return new BufferedOutputStream(file.getOutputStream(options));
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) throws IOException {
    Map<String, List<StorageUri>> bucketToUris =
        uris.stream().collect(Collectors.groupingBy(StorageUri::requiredAuthority));

    IOException ex = null;
    for (List<StorageUri> locations : bucketToUris.values()) {
      DataLakeFileSystemClient fileSystem = clientSupplier.fileSystemClient(locations.get(0));

      // No batch-delete ... yay
      for (StorageUri location : locations) {
        String path = location.requiredPath();
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        try {
          fileSystem.deleteFileIfExists(path);
        } catch (BlobStorageException e) {
          if (e.getStatusCode() != 404) {
            if (ex == null) {
              ex = new IOException(e.getServiceMessage(), e);
            } else {
              ex.addSuppressed(e);
            }
          }
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }

  @Override
  public Optional<String> canResolve(StorageUri uri) {
    try {
      DataLakeFileClient file = clientSupplier.fileClientForLocation(uri);
      return file != null ? Optional.empty() : Optional.of("ADLS client could not be constructed");
    } catch (IllegalArgumentException e) {
      return Optional.of(e.getMessage());
    }
  }

  @Override
  public void configureIcebergWarehouse(
      StorageUri warehouse,
      BiConsumer<String, String> defaultConfig,
      BiConsumer<String, String> configOverride) {
    icebergConfigDefaults(defaultConfig);
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
        .noneMatch(AdlsLocation::isAdlsScheme)) {
      // If there's no ADLS location, no need to do any setup.
      return;
    }

    icebergConfigDefaults(config);
    icebergConfigOverrides(storageLocations, config);
  }

  @Override
  public void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties) {
    properties.accept("fs.native-azure.enabled", "true");
    if (icebergConfig.containsKey(ADLS_READ_BLOCK_SIZE_BYTES)) {
      properties.accept("azure.read-block-size", icebergConfig.get(ADLS_READ_BLOCK_SIZE_BYTES));
    }
    if (icebergConfig.containsKey(ADLS_WRITE_BLOCK_SIZE_BYTES)) {
      properties.accept("azure.write-block-size", icebergConfig.get(ADLS_WRITE_BLOCK_SIZE_BYTES));
    }
  }

  void icebergConfigOverrides(
      StorageLocations storageLocations, BiConsumer<String, String> config) {
    List<AdlsLocation> allLocations =
        Stream.concat(
                storageLocations.writeableLocations().stream(),
                storageLocations.readonlyLocations().stream())
            .map(AdlsLocation::adlsLocation)
            .collect(Collectors.toList());

    Set<String> fileSystems =
        allLocations.stream()
            .map(AdlsLocation::container)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toSet());
    Set<String> storageAccounts =
        allLocations.stream().map(AdlsLocation::storageAccount).collect(Collectors.toSet());

    checkState(
        fileSystems.size() <= 1,
        "Only one ADLS filesystem supported for warehouse %s",
        storageLocations.warehouseLocation());
    checkState(
        storageAccounts.size() == 1,
        "Only one ADLS storage account supported for warehouse %s",
        storageLocations.warehouseLocation());

    String storageAccount = storageAccounts.iterator().next();
    // Iceberg PR https://github.com/apache/iceberg/pull/11504 (since Iceberg 1.7.1) introduced a
    // behavioral change. Before that PR, the whole storage account including the domain name (e.g.
    // myaccount.dfs.core.windows.net) was used in Iceberg's `ADLSLocation` as the lookup key in
    // `AzureProperties.applyClientConfiguration()`. After that PR, only the first part (e.g.
    // myaccount) is used. This is a breaking change, as it requires changes to user configurations.
    int dot = storageAccount.indexOf('.');
    Optional<String> storageAccountShort =
        dot != -1 ? Optional.of(storageAccount.substring(0, dot)) : Optional.empty();

    // We just need one of the locations to resolve the GCS bucket options. Any should work, because
    // it's all for the same table.
    StorageUri loc =
        storageLocations.writeableLocations().isEmpty()
            ? storageLocations.readonlyLocations().get(0)
            : storageLocations.writeableLocations().get(0);

    AdlsOptions adlsOptions = clientSupplier.adlsOptions();
    AdlsNamedFileSystemOptions fileSystemOptions = adlsOptions.resolveOptionsForUri(loc);
    fileSystemOptions
        .endpoint()
        .ifPresent(
            e -> {
              config.accept(ADLS_CONNECTION_STRING_PREFIX + storageAccount, e);
              storageAccountShort.ifPresent(
                  account -> config.accept(ADLS_CONNECTION_STRING_PREFIX + account, e));
            });
    adlsOptions
        .readBlockSize()
        .ifPresent(r -> config.accept(ADLS_READ_BLOCK_SIZE_BYTES, Integer.toString(r)));
    adlsOptions
        .writeBlockSize()
        .ifPresent(s -> config.accept(ADLS_WRITE_BLOCK_SIZE_BYTES, Long.toString(s)));

    clientSupplier
        .generateUserDelegationSas(storageLocations, fileSystemOptions)
        .ifPresent(
            sasToken -> {
              config.accept(ADLS_SAS_TOKEN_PREFIX + storageAccount, sasToken);
              storageAccountShort.ifPresent(
                  account -> config.accept(ADLS_SAS_TOKEN_PREFIX + account, sasToken));
            });

    fileSystemOptions.tableConfigOverrides().forEach(config);
  }

  void icebergConfigDefaults(BiConsumer<String, String> config) {
    config.accept(PYICEBERG_FILE_IO_IMPL, "pyiceberg.io.fsspec.FsspecFileIO");
    config.accept(ICEBERG_FILE_IO_IMPL, "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
  }
}

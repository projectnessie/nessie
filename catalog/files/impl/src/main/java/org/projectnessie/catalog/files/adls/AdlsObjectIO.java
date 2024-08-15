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
import static org.projectnessie.catalog.files.adls.AdlsLocation.adlsLocation;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions;
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
    AdlsLocation location = adlsLocation(uri);

    DataLakeFileSystemClient fileSystem = clientSupplier.fileSystemClient(location);
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
    // Note: the default container is mapped to an empty string
    Map<String, List<AdlsLocation>> bucketToUris =
        uris.stream()
            .map(AdlsLocation::adlsLocation)
            .collect(Collectors.groupingBy(l -> l.container().orElse("")));

    IOException ex = null;
    for (List<AdlsLocation> locations : bucketToUris.values()) {
      DataLakeFileSystemClient fileSystem = clientSupplier.fileSystemClient(locations.get(0));

      // No batch-delete ... yay
      for (AdlsLocation location : locations) {
        String path = location.path();
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
      BooleanSupplier enableRequestSigning,
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
    icebergConfigOverrides(storageLocations, config, true);
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
      StorageLocations storageLocations, BiConsumer<String, String> config, boolean forTable) {
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

    Optional<String> fileSystem =
        fileSystems.isEmpty() ? Optional.empty() : Optional.of(fileSystems.iterator().next());
    String storageAccount = storageAccounts.iterator().next();

    AdlsOptions adlsOptions = clientSupplier.adlsOptions();
    AdlsFileSystemOptions fileSystemOptions = adlsOptions.effectiveOptionsForFileSystem(fileSystem);
    fileSystemOptions
        .endpoint()
        .ifPresent(e -> config.accept(ADLS_CONNECTION_STRING_PREFIX + storageAccount, e));
    adlsOptions
        .readBlockSize()
        .ifPresent(r -> config.accept(ADLS_READ_BLOCK_SIZE_BYTES, Integer.toString(r)));
    adlsOptions
        .writeBlockSize()
        .ifPresent(s -> config.accept(ADLS_WRITE_BLOCK_SIZE_BYTES, Long.toString(s)));

    if (forTable) {
      clientSupplier
          .generateUserDelegationSas(storageLocations, fileSystemOptions)
          .ifPresent(sasToken -> config.accept(ADLS_SAS_TOKEN_PREFIX + storageAccount, sasToken));
    }
  }

  void icebergConfigDefaults(BiConsumer<String, String> config) {
    config.accept(ICEBERG_FILE_IO_IMPL, "org.apache.iceberg.azure.adlsv2.ADLSFileIO");
  }
}

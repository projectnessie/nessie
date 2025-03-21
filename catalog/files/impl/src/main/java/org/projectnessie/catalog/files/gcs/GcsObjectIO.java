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

import static com.google.common.base.Preconditions.checkState;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.time.Duration;
import java.util.ArrayList;
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
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.storage.uri.StorageUri;

public class GcsObjectIO implements ObjectIO {

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

  private final GcsStorageSupplier storageSupplier;

  public GcsObjectIO(GcsStorageSupplier storageSupplier) {
    this.storageSupplier = storageSupplier;
  }

  @Override
  public void ping(StorageUri uri) {
    GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(uri);
    @SuppressWarnings("resource")
    Storage client = storageSupplier.forLocation(bucketOptions);
    client.get(BlobId.of(uri.requiredAuthority(), uri.pathWithoutLeadingTrailingSlash()));
  }

  @Override
  public InputStream readObject(StorageUri uri) {
    GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(uri);
    @SuppressWarnings("resource")
    Storage client = storageSupplier.forLocation(bucketOptions);
    List<BlobSourceOption> sourceOptions = new ArrayList<>();
    bucketOptions
        .decryptionKey()
        .map(
            secretName ->
                storageSupplier
                    .secretsProvider()
                    .getSecret(secretName, SecretType.KEY, KeySecret.class))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(KeySecret::key)
        .map(BlobSourceOption::decryptionKey)
        .ifPresent(sourceOptions::add);
    bucketOptions.userProject().map(BlobSourceOption::userProject).ifPresent(sourceOptions::add);
    ReadChannel reader =
        client.reader(
            BlobId.of(uri.requiredAuthority(), uri.pathWithoutLeadingTrailingSlash()),
            sourceOptions.toArray(new BlobSourceOption[0]));
    bucketOptions.readChunkSize().ifPresent(reader::setChunkSize);
    return Channels.newInputStream(reader);
  }

  @Override
  public OutputStream writeObject(StorageUri uri) {
    GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(uri);
    @SuppressWarnings("resource")
    Storage client = storageSupplier.forLocation(bucketOptions);
    List<BlobWriteOption> writeOptions = new ArrayList<>();

    bucketOptions
        .encryptionKey()
        .map(
            secretName ->
                storageSupplier
                    .secretsProvider()
                    .getSecret(secretName, SecretType.KEY, KeySecret.class))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(KeySecret::key)
        .map(BlobWriteOption::encryptionKey)
        .ifPresent(writeOptions::add);
    bucketOptions.userProject().map(BlobWriteOption::userProject).ifPresent(writeOptions::add);

    BlobInfo blobInfo =
        BlobInfo.newBuilder(
                BlobId.of(uri.requiredAuthority(), uri.pathWithoutLeadingTrailingSlash()))
            .build();
    WriteChannel channel = client.writer(blobInfo, writeOptions.toArray(new BlobWriteOption[0]));
    bucketOptions.writeChunkSize().ifPresent(channel::setChunkSize);
    return Channels.newOutputStream(channel);
  }

  @Override
  public void deleteObjects(List<StorageUri> uris) {
    // TODO this should group by **resolved prefix**
    Map<String, List<StorageUri>> bucketToUris =
        uris.stream().collect(Collectors.groupingBy(StorageUri::requiredAuthority));

    for (List<StorageUri> locations : bucketToUris.values()) {
      GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(locations.get(0));
      @SuppressWarnings("resource")
      Storage client = storageSupplier.forLocation(bucketOptions);

      List<BlobId> blobIds =
          locations.stream()
              .map(
                  location ->
                      BlobId.of(
                          location.requiredAuthority(), location.pathWithoutLeadingTrailingSlash()))
              .collect(Collectors.toList());

      // This is rather a hack to make the `AbstractClients` test pass, because our object storage
      // mock doesn't implement GCS batch requests (yet).
      if (blobIds.size() == 1) {
        client.delete(blobIds.get(0));
      } else {
        client.delete(blobIds);
      }
    }
  }

  @Override
  public Optional<String> canResolve(StorageUri uri) {
    try {
      GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(uri);
      @SuppressWarnings("resource")
      Storage client = storageSupplier.forLocation(bucketOptions);
      return client != null ? Optional.empty() : Optional.of("GCS client could not be constructed");
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
        .noneMatch(GcsLocation::isGcsScheme)) {
      // If there's no GS location, no need to do any setup.
      return;
    }

    icebergConfigDefaults(config);
    GcsBucketOptions bucketOptions = icebergConfigOverrides(storageLocations, config);
    if (bucketOptions.effectiveAuthType() != GcsBucketOptions.GcsAuthType.NONE) {
      storageSupplier
          .generateDelegationToken(storageLocations, bucketOptions)
          .ifPresent(
              t -> {
                config.accept(GCS_OAUTH2_TOKEN, t.token());
                t.expiresAt()
                    .ifPresent(
                        i ->
                            config.accept(
                                GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(i.toEpochMilli())));
              });
    }

    bucketOptions.tableConfigOverrides().forEach(config);
  }

  @Override
  public void trinoSampleConfig(
      StorageUri warehouse,
      Map<String, String> icebergConfig,
      BiConsumer<String, String> properties) {
    properties.accept("fs.native-gcs.enabled", "true");
    properties.accept("gcs.project-id", icebergConfig.get(GCS_PROJECT_ID));
    if (icebergConfig.containsKey(GCS_READ_CHUNK_SIZE)) {
      properties.accept("gcs.read-block-size", icebergConfig.get(GCS_READ_CHUNK_SIZE));
    }
    if (icebergConfig.containsKey(GCS_WRITE_CHUNK_SIZE)) {
      properties.accept("gcs.write-block-size", icebergConfig.get(GCS_WRITE_CHUNK_SIZE));
    }
  }

  GcsBucketOptions icebergConfigOverrides(
      StorageLocations storageLocations, BiConsumer<String, String> config) {
    Set<String> buckets =
        Stream.concat(
                storageLocations.writeableLocations().stream(),
                storageLocations.readonlyLocations().stream())
            .map(StorageUri::requiredAuthority)
            .collect(Collectors.toSet());

    checkState(
        buckets.size() == 1,
        "Only one GCS bucket supported for warehouse %s",
        storageLocations.warehouseLocation());

    // We just need one of the locations to resolve the GCS bucket options. Any should work, because
    // it's all for the same table.
    StorageUri loc =
        storageLocations.writeableLocations().isEmpty()
            ? storageLocations.readonlyLocations().get(0)
            : storageLocations.writeableLocations().get(0);

    GcsBucketOptions bucketOptions = storageSupplier.gcsOptions().resolveOptionsForUri(loc);

    bucketOptions.projectId().ifPresent(p -> config.accept(GCS_PROJECT_ID, p));
    bucketOptions.clientLibToken().ifPresent(t -> config.accept(GCS_CLIENT_LIB_TOKEN, t));
    bucketOptions.host().ifPresent(h -> config.accept(GCS_SERVICE_HOST, h.toString()));
    bucketOptions.userProject().ifPresent(u -> config.accept(GCS_USER_PROJECT, u));
    bucketOptions
        .readChunkSize()
        .ifPresent(rcs -> config.accept(GCS_READ_CHUNK_SIZE, Integer.toString(rcs)));
    bucketOptions
        .writeChunkSize()
        .ifPresent(wcs -> config.accept(GCS_WRITE_CHUNK_SIZE, Integer.toString(wcs)));
    bucketOptions
        .deleteBatchSize()
        .ifPresent(dbs -> config.accept(GCS_DELETE_BATCH_SIZE, Integer.toString(dbs)));
    if (bucketOptions.effectiveAuthType() == GcsBucketOptions.GcsAuthType.NONE) {
      config.accept(GCS_NO_AUTH, "true");
    }
    return bucketOptions;
  }

  void icebergConfigDefaults(BiConsumer<String, String> config) {
    config.accept(PYICEBERG_FILE_IO_IMPL, "pyiceberg.io.fsspec.FsspecFileIO");
    config.accept(ICEBERG_FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO");
  }
}

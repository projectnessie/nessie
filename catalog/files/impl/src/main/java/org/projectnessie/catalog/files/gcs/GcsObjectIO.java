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

import static org.projectnessie.catalog.files.gcs.GcsLocation.gcsLocation;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import org.projectnessie.catalog.files.api.ObjectIO;

public class GcsObjectIO implements ObjectIO {
  private final GcsStorageSupplier storageSupplier;

  public GcsObjectIO(GcsStorageSupplier storageSupplier) {
    this.storageSupplier = storageSupplier;
  }

  @Override
  public InputStream readObject(URI uri) {
    GcsLocation location = gcsLocation(uri);
    GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(location);
    Storage client = storageSupplier.forLocation(bucketOptions);
    List<BlobSourceOption> sourceOptions = new ArrayList<>();
    bucketOptions
        .decryptionKeyRef()
        .map(keyRef -> storageSupplier.secretsProvider().getSecret(keyRef))
        .map(BlobSourceOption::decryptionKey)
        .ifPresent(sourceOptions::add);
    bucketOptions.userProject().map(BlobSourceOption::userProject).ifPresent(sourceOptions::add);
    ReadChannel reader =
        client.reader(
            BlobId.of(location.bucket(), location.path()),
            sourceOptions.toArray(new BlobSourceOption[0]));
    bucketOptions.readChunkSize().ifPresent(reader::setChunkSize);
    return Channels.newInputStream(reader);
  }

  @Override
  public OutputStream writeObject(URI uri) {
    GcsLocation location = gcsLocation(uri);
    GcsBucketOptions bucketOptions = storageSupplier.bucketOptions(location);
    Storage client = storageSupplier.forLocation(bucketOptions);
    List<BlobWriteOption> writeOptions = new ArrayList<>();

    bucketOptions
        .encryptionKeyRef()
        .map(keyRef -> storageSupplier.secretsProvider().getSecret(keyRef))
        .map(BlobWriteOption::encryptionKey)
        .ifPresent(writeOptions::add);
    bucketOptions.userProject().map(BlobWriteOption::userProject).ifPresent(writeOptions::add);

    BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(location.bucket(), location.path())).build();
    WriteChannel channel = client.writer(blobInfo, writeOptions.toArray(new BlobWriteOption[0]));
    bucketOptions.writeChunkSize().ifPresent(channel::setChunkSize);
    return Channels.newOutputStream(channel);
  }

  @Override
  public boolean isValidUri(URI uri) {
    return uri != null && "gs".equals(uri.getScheme());
  }
}

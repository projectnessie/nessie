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
package org.projectnessie.catalog.files.s3;

import static org.projectnessie.catalog.files.gcs.GcsClients.buildSharedHttpTransportFactory;

import com.google.auth.http.HttpTransportFactory;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions;
import org.projectnessie.catalog.files.gcs.GcsObjectIO;
import org.projectnessie.catalog.files.gcs.GcsProgrammaticOptions;
import org.projectnessie.catalog.files.gcs.GcsStorageSupplier;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;

public class TestGcsClients extends AbstractClients {

  @Override
  protected StorageUri buildURI(String bucket, String key) {
    return StorageUri.of(String.format("gs://%s/%s", bucket, key));
  }

  @Override
  protected ObjectIO buildObjectIO(
      ObjectStorageMock.MockServer server1, ObjectStorageMock.MockServer server2) {

    HttpTransportFactory httpTransportFactory = buildSharedHttpTransportFactory();

    GcsProgrammaticOptions.Builder gcsOptions =
        GcsProgrammaticOptions.builder()
            .putBuckets(
                BUCKET_1,
                GcsProgrammaticOptions.GcsPerBucketOptions.builder()
                    .host(server1.getGcsBaseUri())
                    .authType(GcsBucketOptions.GcsAuthType.NONE)
                    .build());
    if (server2 != null) {
      gcsOptions.putBuckets(
          BUCKET_2,
          GcsProgrammaticOptions.GcsPerBucketOptions.builder()
              .host(server2.getGcsBaseUri())
              .authType(GcsBucketOptions.GcsAuthType.NONE)
              .build());
    }

    GcsStorageSupplier supplier =
        new GcsStorageSupplier(httpTransportFactory, gcsOptions.build(), secret -> "secret");

    return new GcsObjectIO(supplier);
  }
}

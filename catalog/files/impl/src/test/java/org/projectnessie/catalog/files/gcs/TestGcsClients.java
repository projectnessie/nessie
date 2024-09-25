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

import static org.projectnessie.catalog.files.gcs.GcsClients.buildSharedHttpTransportFactory;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import com.google.auth.http.HttpTransportFactory;
import java.util.Map;
import org.projectnessie.catalog.files.AbstractClients;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsConfig;
import org.projectnessie.catalog.files.config.ImmutableGcsNamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;

public class TestGcsClients extends AbstractClients {

  @Override
  protected StorageUri buildURI(String bucket, String key) {
    return StorageUri.of(String.format("gs://%s/%s", bucket, key));
  }

  @Override
  protected BackendExceptionMapper.Builder addExceptionHandlers(
      BackendExceptionMapper.Builder builder) {
    return builder.addAnalyzer(GcsExceptionMapper.INSTANCE);
  }

  @Override
  protected ObjectIO buildObjectIO(
      ObjectStorageMock.MockServer server1, ObjectStorageMock.MockServer server2) {

    HttpTransportFactory httpTransportFactory = buildSharedHttpTransportFactory();

    ImmutableGcsOptions.Builder gcsOptions =
        ImmutableGcsOptions.builder()
            .putBucket(
                BUCKET_1,
                ImmutableGcsNamedBucketOptions.builder()
                    .host(server1.getGcsBaseUri())
                    .authType(GcsBucketOptions.GcsAuthType.NONE)
                    .build());
    if (server2 != null) {
      gcsOptions.putBucket(
          BUCKET_2,
          ImmutableGcsNamedBucketOptions.builder()
              .host(server2.getGcsBaseUri())
              .authType(GcsBucketOptions.GcsAuthType.NONE)
              .build());
    }
    ImmutableGcsConfig.Builder gcsConfig = ImmutableGcsConfig.builder();

    GcsStorageSupplier supplier =
        new GcsStorageSupplier(
            httpTransportFactory,
            gcsConfig.build(),
            gcsOptions.build(),
            ResolvingSecretsProvider.builder()
                .putSecretsManager("plain", unsafePlainTextSecretsProvider(Map.of()))
                .build());

    return new GcsObjectIO(supplier);
  }
}

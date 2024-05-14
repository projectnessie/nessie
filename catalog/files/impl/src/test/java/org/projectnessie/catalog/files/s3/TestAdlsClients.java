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

import com.azure.core.http.HttpClient;
import org.projectnessie.catalog.files.adls.AdlsClientSupplier;
import org.projectnessie.catalog.files.adls.AdlsClients;
import org.projectnessie.catalog.files.adls.AdlsConfig;
import org.projectnessie.catalog.files.adls.AdlsObjectIO;
import org.projectnessie.catalog.files.adls.AdlsProgrammaticOptions;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;

public class TestAdlsClients extends AbstractClients {

  @Override
  protected StorageUri buildURI(String bucket, String key) {
    return StorageUri.of(String.format("abfs://%s@storageAccount/%s", bucket, key));
  }

  @Override
  protected ObjectIO buildObjectIO(
      ObjectStorageMock.MockServer server1, ObjectStorageMock.MockServer server2) {
    HttpClient httpClient = AdlsClients.buildSharedHttpClient(AdlsConfig.builder().build());

    AdlsProgrammaticOptions.Builder adlsOptions =
        AdlsProgrammaticOptions.builder()
            .putFileSystems(
                BUCKET_1,
                AdlsProgrammaticOptions.AdlsPerFileSystemOptions.builder()
                    .endpoint(server1.getAdlsGen2BaseUri().toString())
                    .accountNameRef("accountName")
                    .accountKeyRef("accountKey")
                    .build());
    if (server2 != null) {
      adlsOptions.putFileSystems(
          BUCKET_2,
          AdlsProgrammaticOptions.AdlsPerFileSystemOptions.builder()
              .endpoint(server2.getAdlsGen2BaseUri().toString())
              .accountNameRef("accountName")
              .accountKeyRef("accountKey")
              .build());
    }

    AdlsClientSupplier supplier =
        new AdlsClientSupplier(httpClient, adlsOptions.build(), secret -> "secret");

    return new AdlsObjectIO(supplier);
  }
}

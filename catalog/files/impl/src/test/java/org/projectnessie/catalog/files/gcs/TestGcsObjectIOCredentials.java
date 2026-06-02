/*
 * Copyright (C) 2026 Dremio
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.GcsBucketOptions.GcsAuthType;
import org.projectnessie.catalog.files.config.ImmutableGcsBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableGcsOptions;
import org.projectnessie.storage.uri.StorageUri;

class TestGcsObjectIOCredentials {

  @Test
  void emitsStructuredStorageCredentialsForDownscopedToken() {
    StorageUri warehouse = StorageUri.of("gs://bucket/warehouse");
    StorageUri table = StorageUri.of("gs://bucket/warehouse/table");
    StorageUri readonly = StorageUri.of("gs://bucket/warehouse/read");
    StorageLocations storageLocations =
        StorageLocations.storageLocations(warehouse, List.of(table), List.of(readonly));

    var bucketOptions =
        ImmutableGcsBucketOptions.builder().authType(GcsAuthType.ACCESS_TOKEN).build();
    GcsStorageSupplier supplier = mock(GcsStorageSupplier.class);
    when(supplier.gcsOptions())
        .thenReturn(ImmutableGcsOptions.builder().defaultOptions(bucketOptions).build());
    when(supplier.generateDelegationToken(eq(storageLocations), any()))
        .thenReturn(Optional.of(tokenSecret("oauth-token", Instant.ofEpochMilli(12345))));

    Map<String, String> config = new HashMap<>();
    Map<String, Map<String, String>> storageCredentials = new HashMap<>();

    new GcsObjectIO(supplier)
        .configureIcebergTable(
            storageLocations, config::put, storageCredentials::put, ignored -> false, true);

    assertThat(config)
        .containsEntry("gcs.oauth2.token", "oauth-token")
        .containsEntry("gcs.oauth2.token-expires-at", "12345");
    assertThat(storageCredentials)
        .containsKeys(
            "gs://bucket/warehouse", "gs://bucket/warehouse/table", "gs://bucket/warehouse/read");
    assertThat(storageCredentials.values())
        .allSatisfy(
            credential ->
                assertThat(credential)
                    .containsEntry("gcs.oauth2.token", "oauth-token")
                    .containsEntry("gcs.oauth2.token-expires-at", "12345"));
  }
}

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
package org.projectnessie.catalog.files.adls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.files.adls.AdlsClientSupplier.AdlsSasToken;
import org.projectnessie.catalog.files.api.StorageLocations;
import org.projectnessie.catalog.files.config.AdlsFileSystemOptions.AzureAuthType;
import org.projectnessie.catalog.files.config.ImmutableAdlsFileSystemOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsOptions;
import org.projectnessie.catalog.files.config.ImmutableAdlsUserDelegation;
import org.projectnessie.storage.uri.StorageUri;

class TestAdlsObjectIOCredentials {

  @Test
  void emitsStructuredStorageCredentialsForUserDelegationSas() {
    StorageUri warehouse =
        StorageUri.of("abfss://container@account.dfs.core.windows.net/warehouse");
    StorageUri table =
        StorageUri.of("abfss://container@account.dfs.core.windows.net/warehouse/table");
    StorageUri readonly =
        StorageUri.of("abfss://container@account.dfs.core.windows.net/warehouse/read");
    StorageLocations storageLocations =
        StorageLocations.storageLocations(warehouse, List.of(table), List.of(readonly));

    var fileSystemOptions =
        ImmutableAdlsFileSystemOptions.builder()
            .authType(AzureAuthType.APPLICATION_DEFAULT)
            .endpoint("https://account.dfs.core.windows.net")
            .userDelegation(ImmutableAdlsUserDelegation.builder().enable(true).build())
            .build();
    AdlsClientSupplier supplier = mock(AdlsClientSupplier.class);
    when(supplier.adlsOptions())
        .thenReturn(ImmutableAdlsOptions.builder().defaultOptions(fileSystemOptions).build());
    when(supplier.generateUserDelegationSas(eq(storageLocations), any()))
        .thenReturn(Optional.of(new AdlsSasToken("sas-token", Instant.ofEpochMilli(12345))));

    Map<String, String> config = new HashMap<>();
    Map<String, Map<String, String>> storageCredentials = new HashMap<>();

    new AdlsObjectIO(supplier)
        .configureIcebergTable(
            storageLocations, config::put, storageCredentials::put, ignored -> false, true);

    assertThat(config)
        .containsEntry(
            "adls.connection-string.account.dfs.core.windows.net",
            "https://account.dfs.core.windows.net")
        .containsEntry("adls.connection-string.account", "https://account.dfs.core.windows.net")
        .containsEntry("adls.sas-token.account.dfs.core.windows.net", "sas-token")
        .containsEntry("adls.sas-token.account", "sas-token")
        .containsEntry("adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "12345")
        .containsEntry("adls.sas-token-expires-at-ms.account", "12345");
    assertThat(storageCredentials)
        .containsKeys(
            "abfss://container@account.dfs.core.windows.net/warehouse",
            "abfss://container@account.dfs.core.windows.net/warehouse/table",
            "abfss://container@account.dfs.core.windows.net/warehouse/read");
    assertThat(storageCredentials.values())
        .allSatisfy(
            credential ->
                assertThat(credential)
                    .containsEntry(
                        "adls.connection-string.account.dfs.core.windows.net",
                        "https://account.dfs.core.windows.net")
                    .containsEntry(
                        "adls.connection-string.account", "https://account.dfs.core.windows.net")
                    .containsEntry("adls.sas-token.account.dfs.core.windows.net", "sas-token")
                    .containsEntry("adls.sas-token.account", "sas-token")
                    .containsEntry(
                        "adls.sas-token-expires-at-ms.account.dfs.core.windows.net", "12345")
                    .containsEntry("adls.sas-token-expires-at-ms.account", "12345"));
  }
}

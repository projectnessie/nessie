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
package org.projectnessie.versioned.storage.bigtabletests;

import static org.projectnessie.versioned.storage.bigtable.BigTableClientsFactory.applyCommonDataClientSettings;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import java.util.Map;
import org.projectnessie.versioned.storage.bigtable.BigTableBackend;
import org.projectnessie.versioned.storage.bigtable.BigTableBackendConfig;
import org.projectnessie.versioned.storage.bigtable.ImmutableBigTableBackendConfig.Builder;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.testextension.BackendTestFactory;

public abstract class AbstractBigTableBackendTestFactory implements BackendTestFactory {
  protected String projectId;
  protected String instanceId;

  @Override
  public Backend createNewBackend() {
    return createNewBackend(bigtableConfigBuilder().build());
  }

  public Backend createNewBackend(BigTableBackendConfig bigtableConfig) {
    return new BigTableBackend(bigtableConfig);
  }

  public Builder bigtableConfigBuilder() {
    return BigTableBackendConfig.builder()
        .dataClient(buildNewDataClient())
        .tableAdminClient(buildNewTableAdminClient());
  }

  public BigtableDataClient buildNewDataClient() {
    try {
      BigtableDataSettings.Builder settings =
          BigtableDataSettings.newBuilderForEmulator(getEmulatorHost(), getEmulatorPort())
              .setProjectId(projectId)
              .setInstanceId(instanceId)
              .setCredentialsProvider(NoCredentialsProvider.create());

      applyCommonDataClientSettings(settings);

      return BigtableDataClient.create(settings.build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public BigtableTableAdminClient buildNewTableAdminClient() {
    try {
      BigtableTableAdminSettings settings =
          BigtableTableAdminSettings.newBuilderForEmulator(getEmulatorHost(), getEmulatorPort())
              .setProjectId(projectId)
              .setInstanceId(instanceId)
              .setCredentialsProvider(NoCredentialsProvider.create())
              .build();

      return BigtableTableAdminClient.create(settings);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public abstract String getEmulatorHost();

  public abstract int getEmulatorPort();

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of(
        "nessie.version.store.persist.bigtable.emulator-host",
        getEmulatorHost(),
        "nessie.version.store.persist.bigtable.emulator-port",
        Integer.toString(getEmulatorPort()),
        "quarkus.google.cloud.project-id",
        projectId,
        "nessie.version.store.persist.bigtable.instance-id",
        instanceId);
  }
}

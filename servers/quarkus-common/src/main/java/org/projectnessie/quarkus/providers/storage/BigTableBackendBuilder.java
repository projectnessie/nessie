/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.quarkus.providers.storage;

import static org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType.BIGTABLE;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import io.quarkiverse.googlecloudservices.common.GcpBootstrapConfiguration;
import io.quarkiverse.googlecloudservices.common.GcpConfigHolder;
import jakarta.enterprise.context.Dependent;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusBigTableConfig;
import org.projectnessie.quarkus.providers.versionstore.StoreType;
import org.projectnessie.versioned.storage.bigtable.BigTableBackendConfig;
import org.projectnessie.versioned.storage.bigtable.BigTableBackendFactory;
import org.projectnessie.versioned.storage.bigtable.BigTableClientsFactory;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StoreType(BIGTABLE)
@Dependent
public class BigTableBackendBuilder implements BackendBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(BigTableBackendBuilder.class);

  @Inject QuarkusBigTableConfig bigTableConfig;

  @Inject Instance<CredentialsProvider> credentialsProviderInstance;

  @Inject Instance<GcpConfigHolder> gcpConfigHolderInstance;

  @Override
  public Backend buildBackend() {
    CredentialsProvider credentialsProvider = null;
    Exception googleCredentialsException = null;
    // The disableCredentialsLookupForTests() is only used for tests and only to prevent the
    // OpenTelemetry/global-tracer static-field initialization race described in #9866
    if (!bigTableConfig.disableCredentialsLookupForTests()) {
      try {
        credentialsProvider = credentialsProviderInstance.get();
      } catch (Exception e) {
        googleCredentialsException = e;
      }
    }
    GcpConfigHolder gcpConfigHolder = gcpConfigHolderInstance.get();

    GcpBootstrapConfiguration gcpConfiguration = gcpConfigHolder.getBootstrapConfig();

    String projectId =
        gcpConfiguration
            .projectId()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Required Google gRPC configuration quarkus.google.cloud.project-id is missing"));

    if (bigTableConfig.emulatorHost().isEmpty()) {
      if (credentialsProvider == null) {
        if (googleCredentialsException != null) {
          throw new RuntimeException(
              "No Google CredentialsProvider available", googleCredentialsException);
        } else {
          throw new RuntimeException("No Google CredentialsProvider available");
        }
      }
      LOGGER.info(
          "Connecting to Google BigTable using project ID {}, instance ID {}, profile {}, via endpoint {}",
          projectId,
          bigTableConfig.instanceId(),
          bigTableConfig.appProfileId().orElse("(default)"),
          bigTableConfig.endpoint().orElse("(default)"));
    } else {
      LOGGER.info(
          "Connecting to Google BigTable emulator {}:{} using project ID {}, instance ID {}, profile {}",
          bigTableConfig.emulatorHost().get(),
          bigTableConfig.emulatorPort(),
          projectId,
          bigTableConfig.instanceId(),
          bigTableConfig.appProfileId().orElse("(default)"));
    }

    try {

      BigtableDataClient dataClient =
          BigTableClientsFactory.createDataClient(projectId, bigTableConfig, credentialsProvider);

      BigtableTableAdminClient tableAdminClient = null;
      if (bigTableConfig.noTableAdminClient()) {
        LOGGER.info("Google BigTable table admin client creation disabled.");
      } else {
        tableAdminClient =
            BigTableClientsFactory.createTableAdminClient(
                projectId, bigTableConfig, credentialsProvider);

        // Check whether the admin client actually works (Google cloud API access could be
        // disabled). If not, we cannot even check whether tables need to be created, if necessary.
        try {
          tableAdminClient.listTables();
        } catch (PermissionDeniedException e) {
          LOGGER.warn(
              "Google BigTable table admin client cannot list tables due to {}.", e.toString());
          try {
            tableAdminClient.close();
          } finally {
            tableAdminClient = null;
          }
        }
      }

      BigTableBackendFactory factory = new BigTableBackendFactory();
      BigTableBackendConfig c =
          BigTableBackendConfig.builder()
              .dataClient(dataClient)
              .tableAdminClient(tableAdminClient)
              .tablePrefix(bigTableConfig.tablePrefix())
              .totalApiTimeout(bigTableConfig.totalTimeout())
              .build();
      return factory.buildBackend(c);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

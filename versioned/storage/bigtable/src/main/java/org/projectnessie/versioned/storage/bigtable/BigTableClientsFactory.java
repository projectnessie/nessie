/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.EnhancedBigtableStubSettings;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public final class BigTableClientsFactory {

  public static BigtableDataClient createDataClient(
      String projectId,
      BigTableClientsConfig config,
      @Nullable CredentialsProvider credentialsProvider)
      throws IOException {

    if (config.enableTelemetry()) {
      enableOpenCensus();
    }

    BigtableDataSettings.Builder dataSettings =
        (config.emulatorHost().isPresent()
            ? BigtableDataSettings.newBuilderForEmulator(
                    config.emulatorHost().get(), config.emulatorPort())
                .setCredentialsProvider(NoCredentialsProvider.create())
            : BigtableDataSettings.newBuilder().setCredentialsProvider(credentialsProvider));
    dataSettings.setProjectId(projectId).setInstanceId(config.instanceId());
    config.appProfileId().ifPresent(dataSettings::setAppProfileId);
    config.mtlsEndpoint().ifPresent(dataSettings.stubSettings()::setMtlsEndpoint);
    config.quotaProjectId().ifPresent(dataSettings.stubSettings()::setQuotaProjectId);
    config.endpoint().ifPresent(dataSettings.stubSettings()::setEndpoint);

    EnhancedBigtableStubSettings.Builder stubSettings = dataSettings.stubSettings();

    for (RetrySettings.Builder retrySettings :
        List.of(
            stubSettings.readRowSettings().retrySettings(),
            stubSettings.readRowsSettings().retrySettings(),
            stubSettings.bulkReadRowsSettings().retrySettings(),
            stubSettings.mutateRowSettings().retrySettings(),
            stubSettings.bulkMutateRowsSettings().retrySettings(),
            stubSettings.readChangeStreamSettings().retrySettings())) {
      configureDuration(config.totalTimeout(), retrySettings::setTotalTimeout);
      configureDuration(config.initialRpcTimeout(), retrySettings::setInitialRpcTimeout);
      configureDuration(config.maxRpcTimeout(), retrySettings::setMaxRpcTimeout);
      config.rpcTimeoutMultiplier().ifPresent(retrySettings::setRpcTimeoutMultiplier);
      configureDuration(config.initialRetryDelay(), retrySettings::setInitialRetryDelay);
      configureDuration(config.maxRetryDelay(), retrySettings::setMaxRetryDelay);
      config.retryDelayMultiplier().ifPresent(retrySettings::setRetryDelayMultiplier);
      config.maxAttempts().ifPresent(retrySettings::setMaxAttempts);
    }

    InstantiatingGrpcChannelProvider transportChannelProvider =
        (InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider();
    stubSettings.setTransportChannelProvider(
        transportChannelProvider.toBuilder()
            .setChannelPoolSettings(
                channelPoolSettings(transportChannelProvider.getChannelPoolSettings(), config))
            .build());

    applyCommonDataClientSettings(dataSettings);

    return BigtableDataClient.create(dataSettings.build());
  }

  @VisibleForTesting
  static ChannelPoolSettings channelPoolSettings(
      ChannelPoolSettings defaults, BigTableClientsConfig config) {
    ChannelPoolSettings.Builder poolSettings = defaults.toBuilder();
    config.minChannelCount().ifPresent(poolSettings::setMinChannelCount);
    config.maxChannelCount().ifPresent(poolSettings::setMaxChannelCount);
    config.initialChannelCount().ifPresent(poolSettings::setInitialChannelCount);
    config.minRpcsPerChannel().ifPresent(poolSettings::setMinRpcsPerChannel);
    config.maxRpcsPerChannel().ifPresent(poolSettings::setMaxRpcsPerChannel);
    return poolSettings.build();
  }

  // OpenCensus is deprecated in BT for removal
  @SuppressWarnings("deprecation")
  private static void enableOpenCensus() {
    BigtableDataSettings.enableOpenCensusStats();
    BigtableDataSettings.enableGfeOpenCensusStats();
  }

  /**
   * Apply settings that are relevant to both production and test usage. Also called from test
   * factories.
   */
  public static void applyCommonDataClientSettings(BigtableDataSettings.Builder settings) {
    EnhancedBigtableStubSettings.Builder stubSettings = settings.stubSettings();

    stubSettings
        .bulkMutateRowsSettings()
        .setBatchingSettings(
            stubSettings.bulkMutateRowsSettings().getBatchingSettings().toBuilder()
                .setElementCountThreshold((long) BigTableConstants.MAX_BULK_MUTATIONS)
                .build());

    stubSettings
        .bulkReadRowsSettings()
        .setBatchingSettings(
            stubSettings.bulkReadRowsSettings().getBatchingSettings().toBuilder()
                .setElementCountThreshold((long) BigTableConstants.MAX_BULK_READS)
                .build());
  }

  public static BigtableTableAdminClient createTableAdminClient(
      String projectId,
      BigTableClientsConfig config,
      @Nullable CredentialsProvider credentialsProvider)
      throws IOException {

    BigtableTableAdminSettings.Builder adminSettings =
        config.emulatorHost().isPresent()
            ? BigtableTableAdminSettings.newBuilderForEmulator(
                    config.emulatorHost().get(), config.emulatorPort())
                .setCredentialsProvider(NoCredentialsProvider.create())
            : BigtableTableAdminSettings.newBuilder().setCredentialsProvider(credentialsProvider);
    adminSettings.setProjectId(projectId).setInstanceId(config.instanceId());
    config.mtlsEndpoint().ifPresent(adminSettings.stubSettings()::setMtlsEndpoint);
    config.quotaProjectId().ifPresent(adminSettings.stubSettings()::setQuotaProjectId);
    config.endpoint().ifPresent(adminSettings.stubSettings()::setEndpoint);

    return BigtableTableAdminClient.create(adminSettings.build());
  }

  private static void configureDuration(
      Optional<Duration> config, Consumer<org.threeten.bp.Duration> configurable) {
    config.map(Duration::toMillis).map(org.threeten.bp.Duration::ofMillis).ifPresent(configurable);
  }
}

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
package org.projectnessie.server.catalog.secrets;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.runtime.configuration.ConfigurationException;
import io.smallrye.config.SmallRyeConfig;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.Locale;
import java.util.Optional;
import org.projectnessie.catalog.secrets.ImmutableResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsManager;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.cache.CachingSecrets;
import org.projectnessie.catalog.secrets.cache.CachingSecretsBackend;
import org.projectnessie.catalog.secrets.cache.SecretsCacheConfig;
import org.projectnessie.catalog.secrets.smallrye.SmallryeConfigSecretsManager;
import org.projectnessie.quarkus.config.QuarkusSecretsCacheConfig;
import org.projectnessie.quarkus.config.QuarkusSecretsConfig;
import org.projectnessie.quarkus.config.QuarkusSecretsConfig.ExternalSecretsManagerType;
import org.projectnessie.quarkus.providers.RepositoryId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretsProducers {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecretsProducers.class);

  @Produces
  @Singleton
  public SecretsProvider secretsProvider(
      QuarkusSecretsConfig config,
      SmallRyeConfig smallRyeConfig,
      @Any Instance<SecretsManagerBuilder> secretsSupplierBuilders,
      Instance<MeterRegistry> meterRegistry,
      @RepositoryId String repositoryId) {
    var type = config.type();

    SecretsProvider resolving =
        buildResolvingSecretsProvider(smallRyeConfig, secretsSupplierBuilders, type);

    return maybeCache(config, meterRegistry, repositoryId, resolving);
  }

  private SecretsProvider buildResolvingSecretsProvider(
      SmallRyeConfig smallRyeConfig,
      Instance<SecretsManagerBuilder> secretsSupplierBuilders,
      Optional<ExternalSecretsManagerType> type) {

    // Reference secrets via `urn:nessie-secret:quarkus:<secret-name>
    ImmutableResolvingSecretsProvider.Builder providers =
        ResolvingSecretsProvider.builder()
            .putSecretsManager("quarkus", new SmallryeConfigSecretsManager(smallRyeConfig));

    if (type.isEmpty()) {
      LOGGER.info(
          "No external secrets manager has been configured, secrets are retrieved only from the Quarkus configuration.");
      return providers.build();
    }

    Instance<SecretsManagerBuilder> selected =
        secretsSupplierBuilders.select(new SecretsManagerType.Literal(type.get()));

    if (selected.isUnsatisfied()) {
      throw new ConfigurationException(
          "External secrets manager '"
              + type.get().name()
              + "' configured via 'nessie.secrets.type' could not be resolved. Check and fix the configuration.");
    }

    SecretsManager externalManager = selected.get().buildManager();
    providers.putSecretsManager(type.get().name().toLowerCase(Locale.ROOT), externalManager);

    LOGGER.info(
        "External secrets manager '{}' has been configured, secrets can also be retrieved from the Quarkus configuration.",
        type.get().name());
    return providers.build();
  }

  private static SecretsProvider maybeCache(
      QuarkusSecretsConfig config,
      Instance<MeterRegistry> meterRegistry,
      String repositoryId,
      SecretsProvider uncachedProvider) {
    QuarkusSecretsCacheConfig secretsCacheConfig = config.cache();
    if (secretsCacheConfig == null || !secretsCacheConfig.enabled()) {
      return uncachedProvider;
    }

    SecretsCacheConfig.Builder cacheConfig =
        SecretsCacheConfig.builder()
            .maxElements(secretsCacheConfig.maxElements())
            .ttlMillis(secretsCacheConfig.ttl().toMillis());
    if (meterRegistry.isResolvable()) {
      cacheConfig.meterRegistry(meterRegistry.get());
    }
    CachingSecretsBackend cacheBackend = new CachingSecretsBackend(cacheConfig.build());

    return new CachingSecrets(cacheBackend).forRepository(repositoryId, uncachedProvider);
  }

  public void eagerPersistInitialization(
      @Observes StartupEvent event, @SuppressWarnings("unused") SecretsProvider secretsProvider) {
    // no-op
  }
}

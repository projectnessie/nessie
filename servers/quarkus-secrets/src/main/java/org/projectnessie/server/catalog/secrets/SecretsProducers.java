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

import static java.lang.String.format;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.cache.CachingSecrets;
import org.projectnessie.catalog.secrets.cache.CachingSecretsBackend;
import org.projectnessie.catalog.secrets.cache.SecretsCacheConfig;
import org.projectnessie.catalog.secrets.spi.SecretsSupplier;
import org.projectnessie.quarkus.config.QuarkusSecretsConfig;
import org.projectnessie.quarkus.providers.RepositoryId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SecretsProducers {
  private static final Logger LOGGER = LoggerFactory.getLogger(SecretsProducers.class);

  @Produces
  @Singleton
  public SecretsSupplier secretsPSupplier(
      QuarkusSecretsConfig config, @Any Instance<SecretsSupplierBuilder> secretsSupplierBuilders) {
    QuarkusSecretsConfig.SecretsSupplierType type = config.type();

    if (secretsSupplierBuilders.isUnsatisfied()) {
      throw new IllegalStateException("No secrets implementation for " + type);
    }

    return secretsSupplierBuilders.select(new SecretsType.Literal(type)).get().buildSupplier();
  }

  @Produces
  @Singleton
  public SecretsProvider secretsProvider(
      @RepositoryId String repositoryId,
      QuarkusSecretsConfig config,
      SecretsSupplier secretsSupplier,
      @Any Instance<MeterRegistry> meterRegistry) {
    QuarkusSecretsConfig.SecretsSupplierType type = config.type();

    String cacheInfo = "";
    if (type != QuarkusSecretsConfig.SecretsSupplierType.NONE && config.cache().enabled()) {
      SecretsCacheConfig.Builder cacheConfig =
          SecretsCacheConfig.builder()
              .clockNanos(System::nanoTime)
              .ttlMillis(config.cache().ttl().toMillis())
              .maxElements(config.cache().maxElements());
      if (meterRegistry.isResolvable()) {
        cacheConfig.meterRegistry(meterRegistry.get());
      }

      CachingSecretsBackend backend = new CachingSecretsBackend(cacheConfig.build());
      secretsSupplier = new CachingSecrets(backend).forRepository(repositoryId, secretsSupplier);

      cacheInfo =
          format(
              ", with capacity of %d secrets and TTL of %s",
              config.cache().maxElements(), config.cache().ttl());
    }

    LOGGER.info("Using {} secrets provider{}", type, cacheInfo);

    return new SecretsProvider(secretsSupplier);
  }

  public void eagerPersistInitialization(
      @Observes StartupEvent event, SecretsProvider secretsProvider) {
    // no-op
  }
}

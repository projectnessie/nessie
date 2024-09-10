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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Secrets manager and mapping configuration.
 *
 * <p>Currently the following secrets managers are supported:
 *
 * <p>Secrets can always be provided using <a href="#providing-secrets">Quarkus' built-in
 * mechanisms</a>. Additionally, the following external secrets managers can be enabled:
 *
 * <ul>
 *   <li>{@code VAULT} Hashicorp Vault. See the <a
 *       href="https://docs.quarkiverse.io/quarkus-vault/dev/index.html#configuration-reference">Quarkus
 *       docs for Hashicorp Vault</a> for specific information.
 *   <li>{@code AMAZON} AWS Secrets Manager. See the <a
 *       href="https://docs.quarkiverse.io/quarkus-amazon-services/dev/amazon-secretsmanager.html#_configuration_reference">Quarkus
 *       docs for Amazon Services / Secrets Manager</a> for specific information.
 *   <li>{@code AZURE} AWS Secrets Manager. <b>NOT SUPPORTED YET!</b> See the <a
 *       href="https://docs.quarkiverse.io/quarkus-azure-services/dev/quarkus-azure-key-vault.html#_extension_configuration_reference">Quarkus
 *       docs for Azure Key Vault</a> for specific information.
 *   <li>{@code GOOGLE} Google Cloud Secrets Manager. <b>NOT SUPPORTED YET!</b>
 * </ul>
 *
 * <p>For details how secrets are stored, see <a href="#types-of-secrets">below</a>
 */
@ConfigMapping(prefix = "nessie.secrets")
public interface QuarkusSecretsConfig {
  /** Choose the secrets manager to use, defaults to no secrets manager. */
  Optional<ExternalSecretsManagerType> type();

  /**
   * The path/prefix used when accessing secrets from the secrets manager.
   *
   * <p>This setting can be useful, if all Nessie related secrets have the same prefix in your
   * external secrets manager.
   */
  Optional<String> path();

  /** Whether and how to cache retrieved secrets. */
  QuarkusSecretsCacheConfig cache();

  /** Timeout when retrieving a secret from the external secret manager, not supported for AWS. */
  @WithDefault("PT2S")
  Duration getSecretTimeout();

  @Value.Immutable
  abstract class ExternalSecretsManagerType {
    public static final String VAULT = "VAULT";
    public static final String GOOGLE = "GOOGLE";
    public static final String AMAZON = "AMAZON";
    public static final String AZURE = "AZURE";

    @Value.Parameter
    public abstract String name();

    public static ExternalSecretsManagerType valueOf(String name) {
      return ImmutableExternalSecretsManagerType.of(name.toUpperCase(Locale.ROOT));
    }
  }
}

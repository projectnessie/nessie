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
package org.projectnessie.catalog.secrets;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.projectnessie.catalog.secrets.spi.SecretsSupplier;

/**
 * Provides secrets from the configured {@linkplain SecretsSupplier secrets supplier}.
 *
 * <p>The current implementation has functionality that fits the need for Nessie's needs to inject
 * secrets into a configuration object for S3/GCS/ADLS access, which is has a "bucket specific"
 * configuration and a "base/default" configuration.
 */
public class SecretsProvider {

  private final SecretsSupplier secretsSupplier;

  public SecretsProvider(SecretsSupplier secretsSupplier) {
    this.secretsSupplier = secretsSupplier;
  }

  /**
   * Helper to resolve the secrets described via {@link SecretAttribute}s using the given
   * secrets-provider.
   *
   * <p>Individual secrets are resolved in the following order:
   *
   * <ol>
   *   <li>The value of the attribute on the {@code specific} object,
   *   <li>The resolved secret for the specific object (lookup via {@code baseName + '.' +
   *       specificName + '.' + SecretElement.name()},
   *   <li>The value of the attribute on the {@code base} object,
   *   <li>The resolved secret for the base object (lookup via {@code baseName + '.' +
   *       SecretElement.name()},
   * </ol>
   *
   * <p>Callers follow the following pattern:
   *
   * <pre>{@code
   * Config.Builder builder;
   * Config base;
   * Config specific; // nullable
   *
   * builder.from(base).from(specific);
   *
   * Config effective = SecretsProvider.applySecrets(
   *       builder,
   *       "group", base,
   *       "group.specific", specific,
   *       CONFIG_SECRETS)
   *    .build();
   * }</pre>
   */
  public <R, B, S> B applySecrets(
      @Nonnull B builder,
      @Nonnull String baseName,
      @Nonnull R base,
      @Nullable String specificName,
      @Nullable R specific,
      @Nonnull List<SecretAttribute<R, B, ?>> attributes) {

    Set<String> names = new HashSet<>();

    String basePrefix = baseName + '.';
    boolean hasSpecific = specificName != null;
    String specificPrefix = hasSpecific ? basePrefix + specificName + '.' : null;

    @SuppressWarnings({"unchecked", "rawtypes"})
    List<SecretAttribute<R, B, Secret>> casted = (List) attributes;

    for (SecretAttribute<R, B, Secret> attr : casted) {
      Optional<Secret> specificSecret =
          specific != null ? attr.current().apply(specific) : Optional.empty();
      if (specificSecret.isEmpty()) {
        if (hasSpecific) {
          names.add(specificPrefix + attr.name());
        }

        Optional<Secret> baseSecret = attr.current().apply(base);
        if (baseSecret.isEmpty()) {
          names.add(basePrefix + attr.name());
        }
      }
    }

    if (names.isEmpty()) {
      // All secrets present, no need to ask for secrets.
      return builder;
    }

    Map<String, Map<String, String>> secretsMap = secretsSupplier.resolveSecrets(names);

    for (SecretAttribute<R, B, Secret> attr : casted) {
      Optional<? extends Secret> specificSecret =
          specific != null ? attr.current().apply(specific) : Optional.empty();
      if (specificSecret.isEmpty()) {
        Map<String, String> value =
            hasSpecific ? secretsMap.get(specificPrefix + attr.name()) : null;
        if (value != null) {
          applySecretValue(builder, attr, value);
        } else {
          value = secretsMap.get(basePrefix + attr.name());
          if (value != null) {
            applySecretValue(builder, attr, value);
          }
        }
      }
    }

    return builder;
  }

  private <B, R> void applySecretValue(
      B builder, SecretAttribute<R, B, Secret> attr, Map<String, String> value) {
    Secret v = attr.type().fromValueMap(value);
    if (v == null) {
      return;
    }
    attr.applicator().accept(builder, v);
  }
}

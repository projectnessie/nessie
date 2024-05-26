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
package org.projectnessie.catalog.files.secrets;

import static java.util.Collections.emptyMap;

import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.projectnessie.nessie.immutables.NessieImmutable;

public final class SecretsHelper {
  private SecretsHelper() {}

  /**
   * Helper to resolve the secrets described via {@link Specializeable}s using the given
   * secrets-provider.
   *
   * <p>Individual secrets are resolved in the following order:
   *
   * <ol>
   *   <li>The value of the attribute on the {@code specific} object,
   *   <li>The resolved secret for the specific object (lookup via {@code baseName + '.' +
   *       specificName + '.' + Specializeable.name()},
   *   <li>The value of the attribute on the {@code base} object,
   *   <li>The resolved secret for the base object (lookup via {@code baseName + '.' +
   *       Specializeable.name()},
   * </ol>
   */
  public static <R, B> B resolveSecrets(
      @Nonnull SecretsProvider secretsProvider,
      @Nonnull String baseName,
      @Nonnull B builder,
      @Nonnull R base,
      String specificName,
      R specific,
      @Nonnull List<Specializeable<R, B>> secrets) {

    String basePrefix = baseName + '.';
    String specificPrefix = basePrefix + specificName + '.';

    Map<String, String> baseSecrets = new HashMap<>();
    Set<String> names = new HashSet<>();

    for (Specializeable<R, B> secret : secrets) {
      Optional<String> specificSecret =
          specific != null ? secret.current().apply(specific) : Optional.empty();
      if (specificSecret.isPresent()) {
        secret.applicator().accept(builder, specificSecret.get());
      } else {
        names.add(specificPrefix + secret.name());

        Optional<String> baseSecret = secret.current().apply(base);
        if (baseSecret.isPresent()) {
          baseSecrets.put(secret.name(), baseSecret.get());
        } else {
          names.add(basePrefix + secret.name());
        }
      }
    }

    Map<String, String> resolvedSecrets =
        names.isEmpty() ? emptyMap() : secretsProvider.resolveSecrets(names);

    for (Specializeable<R, B> secret : secrets) {
      Optional<String> specificSecret =
          specific != null ? secret.current().apply(specific) : Optional.empty();
      if (specificSecret.isEmpty()) {
        String resolved = resolvedSecrets.get(specificPrefix + secret.name());
        if (resolved == null) {
          resolved = baseSecrets.get(secret.name());
          if (resolved == null) {
            resolved = resolvedSecrets.get(basePrefix + secret.name());
          }
        }
        if (resolved != null) {
          secret.applicator().accept(builder, resolved);
        }
      }
    }

    return builder;
  }

  @NessieImmutable
  public interface Specializeable<R, B> {
    String name();

    Function<R, Optional<String>> current();

    BiConsumer<B, String> applicator();

    static <R, B> Specializeable<R, B> specializable(
        String name, Function<R, Optional<String>> current, BiConsumer<B, String> applicator) {
      return ImmutableSpecializeable.of(name, current, applicator);
    }
  }
}

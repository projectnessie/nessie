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

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Used to map a specific {@link Secret} to an attribute in a configuration object.
 *
 * @param <R> Built configuration object type.
 * @param <B> Builder interface type instance that receives the final value.
 */
@NessieImmutable
public interface SecretAttribute<R, B, S> {
  String name();

  SecretType type();

  Function<R, Optional<S>> current();

  BiConsumer<B, S> applicator();

  /**
   * Constructs a "secret element".
   *
   * @param name name under which this secret it can be retrieved, appended to the "base" and
   *     "specific" names via {@link SecretsProvider#applySecrets(Object, String, Object, String,
   *     Object, List)}
   * @param type type of the secret
   * @param current function to retrieve the current value from a configuration object instance
   * @param applicator function to apply the value to a configuration object builder
   * @return new instance
   * @param <R> Built configuration object type.
   * @param <B> Builder interface type instance that receives the final value.
   * @param <S> Type of secret, for example a {@link BasicCredentials}.
   */
  static <R, B, S extends Secret> SecretAttribute<R, B, S> secretAttribute(
      String name, SecretType type, Function<R, Optional<S>> current, BiConsumer<B, S> applicator) {
    return ImmutableSecretAttribute.of(name, type, current, applicator);
  }
}

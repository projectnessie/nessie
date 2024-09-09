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

import static com.google.common.base.Preconditions.checkArgument;

import jakarta.annotation.Nonnull;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public abstract class ResolvingSecretsProvider implements SecretsProvider {
  abstract Map<String, SecretsManager> secretsManagers();

  @Override
  public <S extends Secret> Optional<S> getSecret(
      @Nonnull URI name, @Nonnull SecretType secretType, @Nonnull Class<S> secretJavaType) {
    String scheme = name.getScheme();
    String next = name.getSchemeSpecificPart();
    checkArgument(
        "urn".equals(scheme) && next != null,
        "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");

    int idxNessieSecret = next.indexOf(':');
    checkArgument(
        idxNessieSecret > 0 && idxNessieSecret != next.length() - 1,
        "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");
    scheme = next.substring(0, idxNessieSecret);
    checkArgument(
        "nessie-secret".equals(scheme),
        "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");

    int idxProvider = next.indexOf(':', idxNessieSecret + 1);
    checkArgument(
        idxProvider > 0 && idxProvider != next.length() - 1,
        "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");
    String manager = next.substring(idxNessieSecret + 1, idxProvider);
    checkArgument(
        !manager.isBlank(),
        "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");

    next = next.substring(idxProvider + 1);
    checkArgument(
        !next.isBlank() && next.charAt(0) != ':',
        "Invalid secret URI, must be in the form 'urn:nessie-secret:<provider>:<secret-name>'");

    SecretsManager secretsProvider = secretsManagers().get(manager);
    if (secretsProvider == null) {
      return Optional.empty();
    }
    return secretsProvider.getSecret(next, secretType, secretJavaType);
  }

  public static ImmutableResolvingSecretsProvider.Builder builder() {
    return ImmutableResolvingSecretsProvider.builder();
  }
}

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
package org.projectnessie.catalog.secrets.vault;

import io.quarkus.vault.VaultKVSecretReactiveEngine;
import io.quarkus.vault.client.VaultClientException;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import java.util.Map;
import org.projectnessie.catalog.secrets.AbstractMapBasedSecretsManager;

public class VaultSecretsManager extends AbstractMapBasedSecretsManager {
  private final VaultKVSecretReactiveEngine secretEngine;
  private final String prefix;
  private final Duration getSecretTimeout;

  public VaultSecretsManager(
      VaultKVSecretReactiveEngine secretEngine, String prefix, Duration getSecretTimeout) {
    this.secretEngine = secretEngine;
    this.prefix = prefix;
    this.getSecretTimeout = getSecretTimeout;
  }

  @Override
  public Map<String, String> resolveSecret(@Nonnull String name) {
    return secretEngine
        .readSecret(path(name))
        .onFailure(
            e -> {
              if (!(e instanceof VaultClientException)) {
                return false;
              }
              VaultClientException ex = (VaultClientException) e;
              return 404 == ex.getStatus();
            })
        .recoverWithNull()
        .await()
        .atMost(getSecretTimeout);
  }

  private String path(String name) {
    return prefix + name.replace('.', '/');
  }
}

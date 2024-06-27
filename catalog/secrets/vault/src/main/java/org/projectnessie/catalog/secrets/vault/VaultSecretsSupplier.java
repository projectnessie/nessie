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
import io.smallrye.mutiny.Uni;
import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.spi.SecretsSupplier;

public class VaultSecretsSupplier implements SecretsSupplier {
  private final VaultKVSecretReactiveEngine secretEngine;
  private final String prefix;
  private final Duration getSecretTimeout;

  public VaultSecretsSupplier(
      VaultKVSecretReactiveEngine secretEngine, String prefix, Duration getSecretTimeout) {
    this.secretEngine = secretEngine;
    this.prefix = prefix;
    this.getSecretTimeout = getSecretTimeout;
  }

  @Override
  public Map<String, Secret> resolveSecrets(Map<String, SecretType> toResolve) {
    return Uni.join()
        .all(
            toResolve.keySet().stream()
                .map(
                    name ->
                        secretEngine
                            .readSecret(path(name))
                            .map(json -> Map.entry(name, json))
                            .onFailure(
                                e -> {
                                  if (!(e instanceof VaultClientException)) {
                                    return false;
                                  }
                                  VaultClientException ex = (VaultClientException) e;
                                  return 404 == ex.getStatus();
                                })
                            .recoverWithItem(Map.entry(name, Map.of())))
                .collect(Collectors.toList()))
        .andCollectFailures()
        .await()
        .atMost(getSecretTimeout)
        .stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(
            Collectors.toMap(
                Map.Entry::getKey, e -> toResolve.get(e.getKey()).fromValueMap(e.getValue())));
  }

  private String path(String name) {
    return prefix + name.replace('.', '/');
  }
}

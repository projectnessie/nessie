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
package org.projectnessie.catalog.secrets.azure;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.security.keyvault.secrets.SecretAsyncClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.spi.SingleValueSecretsSupplier;

public class AzureSecretsSupplier extends SingleValueSecretsSupplier {
  private final SecretAsyncClient client;
  private final String prefix;
  private final long timeout;

  public AzureSecretsSupplier(SecretAsyncClient client, String prefix, Duration getSecretTimeout) {
    this.client = client;
    this.prefix = prefix;
    this.timeout = getSecretTimeout.toMillis();
  }

  @Override
  protected Map<String, String> resolveSingleValueSecrets(Map<String, SecretType> toResolve) {
    if (toResolve.isEmpty()) {
      return Map.of();
    }

    List<CompletableFuture<Map.Entry<String, String>>> futures = new ArrayList<>();

    for (String name : toResolve.keySet()) {
      String secretId = nameToSecretId(name);
      futures.add(
          client
              .getSecret(secretId)
              .map(s -> Map.entry(name, s.getValue()))
              .toFuture()
              .exceptionally(
                  t -> {
                    if (t instanceof ResourceNotFoundException) {
                      return null;
                    }
                    if (t instanceof RuntimeException) {
                      throw (RuntimeException) t;
                    }
                    throw new RuntimeException(t);
                  }));
    }

    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(timeout, MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    return futures.stream()
        .map(f -> f.getNow(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private String nameToSecretId(String name) {
    return prefix + name;
  }
}

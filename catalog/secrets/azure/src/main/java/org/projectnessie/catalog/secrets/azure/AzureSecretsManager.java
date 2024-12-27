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
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.projectnessie.catalog.secrets.AbstractStringBasedSecretsManager;

public class AzureSecretsManager extends AbstractStringBasedSecretsManager {
  private final SecretAsyncClient client;
  private final String prefix;
  private final long timeout;

  public AzureSecretsManager(SecretAsyncClient client, String prefix, Duration getSecretTimeout) {
    this.client = client;
    this.prefix = prefix;
    this.timeout = getSecretTimeout.toMillis();
  }

  @Override
  protected String resolveSecretString(String name) {
    String secretId = nameToSecretId(name);

    CompletableFuture<KeyVaultSecret> future =
        client
            .getSecret(secretId)
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
                });

    try {
      return Optional.ofNullable(future.get(timeout, MILLISECONDS))
          .map(KeyVaultSecret::getValue)
          .orElse(null);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  private String nameToSecretId(String name) {
    return prefix + name;
  }
}

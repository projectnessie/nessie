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
package org.projectnessie.catalog.secrets.gcs;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.spi.SingleValueSecretsSupplier;

public class GcsSecretsSupplier extends SingleValueSecretsSupplier {
  private final String prefix;
  private final SecretManagerServiceClient client;
  private final long getSecretTimeout;

  public GcsSecretsSupplier(
      SecretManagerServiceClient client, String prefix, Duration getSecretTimeout) {
    this.client = client;
    this.prefix = prefix;
    this.getSecretTimeout = getSecretTimeout.toMillis();
  }

  @Override
  protected Map<String, String> resolveSingleValueSecrets(Map<String, SecretType> toResolve) {
    if (toResolve.isEmpty()) {
      return Map.of();
    }

    Set<String> toResolveKeys = toResolve.keySet();

    if (toResolve.size() == 1) {
      String name = toResolveKeys.iterator().next();
      String secretId = nameToSecretId(name);
      AccessSecretVersionResponse response = client.accessSecretVersion(request(secretId));
      return response.hasPayload() ? Map.of(name, secret(response)) : Map.of();
    }

    Map<String, String> secretIdToNameMap = new HashMap<>();

    for (String name : toResolveKeys) {
      String secretId = nameToSecretId(name);
      secretIdToNameMap.put(secretId, name);
    }

    List<ApiFuture<AccessSecretVersionResponse>> futures =
        secretIdToNameMap.keySet().stream()
            .map(id -> client.accessSecretVersionCallable().futureCall(request(id)))
            .collect(Collectors.toList());

    List<AccessSecretVersionResponse> results;
    try {
      results = ApiFutures.successfulAsList(futures).get(getSecretTimeout, MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }

    return results.stream()
        .collect(Collectors.toMap(resp -> secretIdToNameMap.get(resp.getName()), this::secret));
  }

  private String secret(AccessSecretVersionResponse response) {
    return response.getPayload().getData().toStringUtf8();
  }

  private AccessSecretVersionRequest request(String name) {
    return AccessSecretVersionRequest.newBuilder().setName(name).build();
  }

  private String nameToSecretId(String name) {
    return prefix + name;
  }
}

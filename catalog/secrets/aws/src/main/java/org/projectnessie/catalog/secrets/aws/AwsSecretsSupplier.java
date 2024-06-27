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
package org.projectnessie.catalog.secrets.aws;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.spi.SingleValueSecretsSupplier;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.BatchGetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.SecretValueEntry;

public class AwsSecretsSupplier extends SingleValueSecretsSupplier {
  private final SecretsManagerClient secretsManagerClient;
  private final String prefix;

  public AwsSecretsSupplier(
      SecretsManagerClient secretsManagerClient, String prefix, Duration getSecretTimeout) {
    this.secretsManagerClient = secretsManagerClient;
    this.prefix = prefix;
  }

  @Override
  protected Map<String, String> resolveSingleValueSecrets(Map<String, SecretType> toResolve) {
    if (toResolve.isEmpty()) {
      return Map.of();
    }

    Map<String, String> secretIdToNameMap = new HashMap<>();

    for (String name : toResolve.keySet()) {
      String secretId = nameToSecretId(name);
      secretIdToNameMap.put(secretId, name);
    }

    return secretsManagerClient
        .batchGetSecretValue(
            BatchGetSecretValueRequest.builder().secretIdList(secretIdToNameMap.keySet()).build())
        .secretValues()
        .stream()
        .collect(
            Collectors.toMap(
                sv -> secretIdToNameMap.get(sv.name()), SecretValueEntry::secretString));
  }

  private String nameToSecretId(String name) {
    return prefix + name;
  }
}

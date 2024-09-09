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

import org.projectnessie.catalog.secrets.AbstractStringBasedSecretsManager;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;

public class AwsSecretsManager extends AbstractStringBasedSecretsManager {
  private final SecretsManagerClient secretsManagerClient;
  private final String prefix;

  public AwsSecretsManager(SecretsManagerClient secretsManagerClient, String prefix) {
    this.secretsManagerClient = secretsManagerClient;
    this.prefix = prefix;
  }

  @Override
  protected String resolveSecretString(String name) {
    String secretId = nameToSecretId(name);

    try {
      return secretsManagerClient
          .getSecretValue(GetSecretValueRequest.builder().secretId(secretId).build())
          .secretString();
    } catch (ResourceNotFoundException nf) {
      return null;
    }
  }

  private String nameToSecretId(String name) {
    return prefix + name;
  }
}

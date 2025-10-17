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

import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import java.net.URI;
import java.time.Instant;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITAwsSecretsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITAwsSecretsProvider.class);

  @InjectSoftAssertions SoftAssertions soft;

  @Container
  static LocalStackContainer localstack =
      new LocalStackContainer(
              ContainerSpecHelper.builder()
                  .name("localstack")
                  .containerClass(ITAwsSecretsProvider.class)
                  .build()
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("localstack/localstack"))
          .withLogConsumer(c -> LOGGER.info("[LOCALSTACK] {}", c.getUtf8StringWithoutLineEnding()))
          .withServices("secretsmanager");

  @Test
  public void awsSecretsManager() {
    URI secretsManagerEndpoint = localstack.getEndpoint();
    try (SecretsManagerClient client =
        SecretsManagerClient.builder()
            .endpointOverride(secretsManagerEndpoint)
            .region(Region.of(localstack.getRegion()))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        localstack.getAccessKey(), localstack.getSecretKey())))
            .build()) {

      String instantStr = "2024-06-05T20:38:16Z";
      Instant instant = Instant.parse(instantStr);

      KeySecret keySecret = keySecret("secret-foo");
      BasicCredentials basicCred = basicCredentials("bar-name", "bar-secret");
      TokenSecret tokenSec = tokenSecret("the-token", instant);

      String key = "key";
      String basic = "basic";
      String tok = "tok";

      client.createSecret(
          CreateSecretRequest.builder().name("key").secretString("secret-foo").build());
      client.createSecret(
          CreateSecretRequest.builder()
              .name("basic")
              .secretString("{\"name\": \"bar-name\", \"secret\": \"bar-secret\"}")
              .build());
      client.createSecret(
          CreateSecretRequest.builder()
              .name("tok")
              .secretString("{\"token\": \"the-token\", \"expiresAt\": \"" + instantStr + "\"}")
              .build());

      AwsSecretsManager secretsProvider = new AwsSecretsManager(client, "");

      soft.assertThat(secretsProvider.getSecret(key, SecretType.KEY, KeySecret.class))
          .get()
          .asInstanceOf(type(KeySecret.class))
          .extracting(KeySecret::key)
          .isEqualTo(keySecret.key());
      soft.assertThat(secretsProvider.getSecret(basic, SecretType.BASIC, BasicCredentials.class))
          .get()
          .asInstanceOf(type(BasicCredentials.class))
          .extracting(BasicCredentials::name, BasicCredentials::secret)
          .containsExactly(basicCred.name(), basicCred.secret());
      soft.assertThat(secretsProvider.getSecret(tok, SecretType.EXPIRING_TOKEN, TokenSecret.class))
          .get()
          .asInstanceOf(type(TokenSecret.class))
          .extracting(TokenSecret::token, TokenSecret::expiresAt)
          .containsExactly(tokenSec.token(), tokenSec.expiresAt());

      soft.assertThat(secretsProvider.getSecret("not-there", SecretType.KEY, KeySecret.class))
          .isEmpty();
      soft.assertThat(secretsProvider.getSecret("nope", SecretType.BASIC, BasicCredentials.class))
          .isEmpty();
    }
  }
}

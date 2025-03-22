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

import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import com.azure.security.keyvault.secrets.SecretAsyncClient;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainer;
import com.github.nagyesta.lowkeyvault.testcontainers.LowkeyVaultContainerBuilder;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITAzureSecretsProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITAzureSecretsProvider.class);

  @InjectSoftAssertions SoftAssertions soft;

  @Container
  static LowkeyVaultContainer lowkeyVault =
      LowkeyVaultContainerBuilder.lowkeyVault(
              ContainerSpecHelper.builder()
                  .name("lowkey-vault")
                  .containerClass(ITAzureSecretsProvider.class)
                  .build()
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("nagyesta/lowkey-vault"))
          .build()
          .withLogConsumer(
              c -> LOGGER.info("[LOWKEY-VAULT] {}", c.getUtf8StringWithoutLineEnding()));

  @Test
  public void azureSecrets() {
    final SecretAsyncClient client =
        lowkeyVault.getClientFactory().getSecretClientBuilderForDefaultVault().buildAsyncClient();

    String instantStr = "2024-06-05T20:38:16Z";
    Instant instant = Instant.parse(instantStr);

    KeySecret keySecret = keySecret("secret-foo");
    BasicCredentials basicCred = basicCredentials("bar-name", "bar-secret");
    TokenSecret tokenSec = tokenSecret("the-token", instant);

    String key = "key";
    String basic = "basic";
    String tok = "tok";

    client.setSecret(key, "secret-foo").block(Duration.of(1, ChronoUnit.MINUTES));
    client
        .setSecret(basic, "{\"name\": \"bar-name\", \"secret\": \"bar-secret\"}")
        .block(Duration.of(1, ChronoUnit.MINUTES));
    client
        .setSecret(tok, "{\"token\": \"the-token\", \"expiresAt\": \"" + instantStr + "\"}")
        .block(Duration.of(1, ChronoUnit.MINUTES));

    AzureSecretsManager secretsProvider =
        new AzureSecretsManager(client, "", Duration.ofMinutes(1));

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

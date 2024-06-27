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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.TokenSecret.tokenSecret;

import io.quarkus.vault.VaultKVSecretReactiveEngine;
import io.quarkus.vault.client.VaultClient;
import io.quarkus.vault.runtime.VaultConfigHolder;
import io.quarkus.vault.runtime.VaultKvManager;
import io.quarkus.vault.runtime.client.VaultClientProducer;
import io.quarkus.vault.runtime.config.VaultRuntimeConfig;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
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
import org.testcontainers.vault.VaultContainer;

@Testcontainers
@ExtendWith(SoftAssertionsExtension.class)
public class ITVaultSecretsSupplier {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITVaultSecretsSupplier.class);

  public static final String VAULT_ROOT_TOKEN = "root";
  public static final String NESSIE_SECRETS_PATH = "apps/nessie/secrets";

  @InjectSoftAssertions SoftAssertions soft;

  @SuppressWarnings("resource")
  @Container
  static VaultContainer<?> vaultContainer =
      new VaultContainer<>(
              ContainerSpecHelper.builder()
                  .name("vault")
                  .containerClass(ITVaultSecretsSupplier.class)
                  .build()
                  .dockerImageName(null)
                  .asCompatibleSubstituteFor("vault"))
          .withLogConsumer(c -> LOGGER.info("[VAULT] {}", c.getUtf8StringWithoutLineEnding()))
          .withVaultToken(VAULT_ROOT_TOKEN);

  @Test
  public void vault() {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .setAddDefaultSources(false)
            .setAddDiscoveredSources(false)
            .withMapping(VaultRuntimeConfig.class)
            .withSources(
                new PropertiesConfigSource(
                    Map.of(
                        "quarkus.vault.url",
                        vaultContainer.getHttpHostAddress(),
                        "quarkus.vault.authentication.client-token",
                        VAULT_ROOT_TOKEN,
                        "quarkus.vault.secret-config-kv-path",
                        NESSIE_SECRETS_PATH),
                    "configSource",
                    100))
            .build();

    VaultRuntimeConfig runtimeConfig = config.getConfigMapping(VaultRuntimeConfig.class);

    VaultConfigHolder configHolder = new VaultConfigHolder();
    configHolder.setVaultRuntimeConfig(runtimeConfig);
    VaultClient client = new VaultClientProducer().privateVaultClient(configHolder, true);

    VaultKVSecretReactiveEngine vault = new VaultKvManager(client, configHolder);

    Instant instant = Instant.parse("2024-06-05T20:38:16Z");

    KeySecret keySecret = keySecret("secret-foo");
    BasicCredentials basicCred = basicCredentials("bar-name", "bar-secret");
    TokenSecret tokenSec = tokenSecret("the-token", instant);

    vault.writeSecret("key", Map.of("key", keySecret.key())).await().indefinitely();
    vault
        .writeSecret("basic", Map.of("name", basicCred.name(), "secret", basicCred.secret()))
        .await()
        .indefinitely();
    vault
        .writeSecret(
            "tok",
            Map.of(
                "token",
                tokenSec.token(),
                "expiresAt",
                tokenSec.expiresAt().orElseThrow().toString()))
        .await()
        .indefinitely();

    VaultSecretsSupplier secretsSupplier =
        new VaultSecretsSupplier(vault, "", Duration.ofMinutes(1));

    soft.assertThat(
            secretsSupplier.resolveSecrets(
                Map.of(
                    "key",
                    SecretType.KEY,
                    "basic",
                    SecretType.BASIC,
                    "tok",
                    SecretType.EXPIRING_TOKEN)))
        .hasSize(3)
        .hasEntrySatisfying(
            "key",
            s ->
                assertThat(s)
                    .asInstanceOf(type(KeySecret.class))
                    .extracting(KeySecret::key)
                    .isEqualTo(keySecret.key()))
        .hasEntrySatisfying(
            "basic",
            s ->
                assertThat(s)
                    .asInstanceOf(type(BasicCredentials.class))
                    .extracting(BasicCredentials::name, BasicCredentials::secret)
                    .containsExactly(basicCred.name(), basicCred.secret()))
        .hasEntrySatisfying(
            "tok",
            s ->
                assertThat(s)
                    .asInstanceOf(type(TokenSecret.class))
                    .extracting(TokenSecret::token, TokenSecret::expiresAt)
                    .containsExactly(tokenSec.token(), tokenSec.expiresAt()));

    soft.assertThat(
            secretsSupplier.resolveSecrets(
                Map.of(
                    "key",
                    SecretType.KEY,
                    "nope",
                    SecretType.BASIC,
                    "basic",
                    SecretType.BASIC,
                    "not-there",
                    SecretType.KEY,
                    "tok",
                    SecretType.EXPIRING_TOKEN)))
        .hasSize(3)
        .hasEntrySatisfying(
            "key",
            s ->
                assertThat(s)
                    .asInstanceOf(type(KeySecret.class))
                    .extracting(KeySecret::key)
                    .isEqualTo(keySecret.key()))
        .hasEntrySatisfying(
            "basic",
            s ->
                assertThat(s)
                    .asInstanceOf(type(BasicCredentials.class))
                    .extracting(BasicCredentials::name, BasicCredentials::secret)
                    .containsExactly(basicCred.name(), basicCred.secret()))
        .hasEntrySatisfying(
            "tok",
            s ->
                assertThat(s)
                    .asInstanceOf(type(TokenSecret.class))
                    .extracting(TokenSecret::token, TokenSecret::expiresAt)
                    .containsExactly(tokenSec.token(), tokenSec.expiresAt()));

    soft.assertThat(
            secretsSupplier.resolveSecrets(
                Map.of("nope", SecretType.BASIC, "not-there", SecretType.KEY)))
        .isEmpty();
  }
}

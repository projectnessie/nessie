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
package org.projectnessie.server.secrets;

import static io.quarkus.vault.client.api.VaultAuthAccessor.DEFAULT_USERPASS_MOUNT_PATH;
import static io.quarkus.vault.client.api.VaultSecretsAccessor.DEFAULT_KV2_MOUNT_PATH;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.AnnotatedAndMatchesType;
import io.quarkus.vault.client.VaultClient;
import io.quarkus.vault.client.api.VaultAuthAccessor;
import io.quarkus.vault.client.api.VaultSecretsAccessor;
import io.quarkus.vault.client.api.VaultSysAccessor;
import io.quarkus.vault.client.api.auth.userpass.VaultAuthUserPassUpdateUserParams;
import io.quarkus.vault.client.api.secrets.kv2.VaultSecretsKV2UpdateSecretOptions;
import io.quarkus.vault.client.http.jdk.JDKVaultHttpClient;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.projectnessie.quarkus.config.QuarkusSecretsConfig.ExternalSecretsManagerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.vault.VaultContainer;

public class VaultTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(VaultTestResourceLifecycleManager.class);

  public static final String VAULT_ROOT_TOKEN = "root";
  public static final String VAULT_USERPASS_AUTH_MOUNT = DEFAULT_USERPASS_MOUNT_PATH;
  public static final String VAULT_MOUNT = DEFAULT_KV2_MOUNT_PATH;
  public static final String NESSIE_SECRETS_PATH = "apps/nessie/secrets";
  public static final String NESSIE_SECRETS_CONFIG = NESSIE_SECRETS_PATH + "/config";
  public static final String VAULT_USERNAME = "nessie_user";
  public static final String VAULT_PASSWORD = "nessie_password";
  public static final String VAULT_POLICY = "nessie-policy";

  private volatile VaultContainer<?> vaultContainer;
  private volatile VaultSecretsAccessor secretsAccessor;

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface VaultUri {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface VaultToken {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface VaultMountPath {}

  @SuppressWarnings("resource")
  @Override
  public Map<String, String> start() {
    vaultContainer =
        new VaultContainer<>(
                ContainerSpecHelper.builder()
                    .name("vault")
                    .containerClass(VaultTestResourceLifecycleManager.class)
                    .build()
                    .dockerImageName(null)
                    .asCompatibleSubstituteFor("vault"))
            .withLogConsumer(c -> LOGGER.info("[VAULT] {}", c.getUtf8StringWithoutLineEnding()))
            .withVaultToken(VAULT_ROOT_TOKEN)
            .withInitCommand("auth enable userpass");

    vaultContainer.start();

    try {
      VaultClient vaultClient =
          VaultClient.builder()
              .clientToken(VAULT_ROOT_TOKEN)
              .baseUrl(vaultContainer.getHttpHostAddress())
              .executor(new JDKVaultHttpClient(HttpClient.newHttpClient()))
              .build();

      // Following the example at
      // https://docs.quarkiverse.io/quarkus-vault/dev/index.html#_starting_vault

      VaultSysAccessor sysAccessor = new VaultSysAccessor(vaultClient);
      VaultAuthAccessor authAccessor = new VaultAuthAccessor(vaultClient);
      secretsAccessor = new VaultSecretsAccessor(vaultClient);

      secretsAccessor
          .kv2(VAULT_MOUNT)
          .updateSecret(
              NESSIE_SECRETS_CONFIG,
              new VaultSecretsKV2UpdateSecretOptions(),
              Map.of("some", "thing"))
          .toCompletableFuture()
          .get(10, SECONDS);

      sysAccessor
          .policy()
          .update(
              VAULT_POLICY,
              format(
                  "path \"%s/data/%s/*\" {\n  capabilities = [\"read\", \"create\"]\n}\n",
                  VAULT_MOUNT, NESSIE_SECRETS_PATH))
          .toCompletableFuture()
          .get(10, SECONDS);

      authAccessor
          .userPass(VAULT_USERPASS_AUTH_MOUNT)
          .updateUser(
              VAULT_USERNAME,
              new VaultAuthUserPassUpdateUserParams()
                  .setPassword(VAULT_PASSWORD)
                  .setTokenPolicies(List.of(VAULT_POLICY)))
          .toCompletableFuture()
          .get(10, SECONDS);

      return ImmutableMap.<String, String>builder()
          .put("quarkus.vault.url", vaultContainer.getHttpHostAddress())
          .put("quarkus.vault.tls.skip-verify", "true")
          .put(
              // "quarkus.vault.authentication.client-token",
              // VAULT_ROOT_TOKEN,
              "quarkus.vault.authentication.userpass.username", VAULT_USERNAME)
          .put("quarkus.vault.authentication.userpass.password", VAULT_PASSWORD)
          .put("quarkus.vault.authentication.userpass.auth-mount-path", VAULT_USERPASS_AUTH_MOUNT)
          .put("quarkus.vault.kv-secret-engine-mount-path", VAULT_MOUNT)
          .put("quarkus.vault.secret-config-kv-path", NESSIE_SECRETS_CONFIG)
          .put("nessie.secrets.type", ExternalSecretsManagerType.VAULT)
          .put("nessie.secrets.path", NESSIE_SECRETS_PATH)
          .build();
    } catch (Exception e) {
      RuntimeException t = new RuntimeException(e);
      try {
        stop();
      } catch (Exception ex) {
        t.addSuppressed(ex);
      }
      throw t;
    }
  }

  @Override
  public void stop() {
    if (vaultContainer != null) {
      try {
        vaultContainer.stop();
      } finally {
        vaultContainer = null;
      }
    }
  }

  void updateSecrets(Map<String, Secret> secrets, Map<String, SecretType> secretTypes) {
    CompletableFuture<?>[] futures =
        secrets.entrySet().stream()
            .map(
                e -> {
                  Secret secret = e.getValue();

                  Map<String, Object> value;
                  if (secret instanceof KeySecret key) {
                    value = Map.of("key", key.key());
                  } else if (secret instanceof BasicCredentials basic) {
                    value = Map.of("name", basic.name(), "secret", basic.secret());
                  } else if (secret instanceof TokenSecret token) {
                    value =
                        token.expiresAt().isPresent()
                            ? Map.of(
                                "token",
                                token.token(),
                                "expiresAt",
                                token.expiresAt().get().toString())
                            : Map.of("token", token.token());
                  } else {
                    throw new RuntimeException("Unsupported secret type: " + secret);
                  }

                  return secretsAccessor
                      .kv2(VAULT_MOUNT)
                      .updateSecret(
                          NESSIE_SECRETS_PATH + "/" + e.getKey().replace('.', '/'),
                          new VaultSecretsKV2UpdateSecretOptions(),
                          value);
                })
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new);
    try {
      CompletableFuture.allOf(futures).get(10, SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        vaultContainer.getHttpHostAddress(),
        new AnnotatedAndMatchesType(VaultUri.class, String.class));
    testInjector.injectIntoFields(
        VAULT_ROOT_TOKEN, new AnnotatedAndMatchesType(VaultToken.class, String.class));
    testInjector.injectIntoFields(
        VAULT_MOUNT, new AnnotatedAndMatchesType(VaultMountPath.class, String.class));
    testInjector.injectIntoFields(
        (SecretsUpdateHandler) this::updateSecrets,
        new AnnotatedAndMatchesType(SecretsUpdater.class, SecretsUpdateHandler.class));
  }
}

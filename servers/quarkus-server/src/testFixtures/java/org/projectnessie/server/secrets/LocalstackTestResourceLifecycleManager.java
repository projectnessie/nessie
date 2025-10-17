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

import static java.lang.String.format;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.AnnotatedAndMatchesType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.util.Map;
import org.projectnessie.catalog.secrets.BasicCredentials;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.Secret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.TokenSecret;
import org.projectnessie.nessie.testing.containerspec.ContainerSpecHelper;
import org.projectnessie.quarkus.config.QuarkusSecretsConfig.ExternalSecretsManagerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;

public class LocalstackTestResourceLifecycleManager implements QuarkusTestResourceLifecycleManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LocalstackTestResourceLifecycleManager.class);

  public static final String NESSIE_SECRETS_PATH = "nessie.secrets";

  private volatile LocalStackContainer localstack;
  private volatile SecretsManagerClient secretsManagerClient;

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface AwsSecretsManagerEndpoint {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface AwsSecretsManagerRegion {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface AwsSecretsManagerAccessKey {}

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface AwsSecretsManagerSecretAccessKey {}

  @Override
  public Map<String, String> start() {
    localstack =
        new LocalStackContainer(
                ContainerSpecHelper.builder()
                    .name("localstack")
                    .containerClass(LocalstackTestResourceLifecycleManager.class)
                    .build()
                    .dockerImageName(null)
                    .asCompatibleSubstituteFor("localstack/localstack"))
            .withLogConsumer(
                c -> LOGGER.info("[LOCALSTACK] {}", c.getUtf8StringWithoutLineEnding()))
            .withServices("secretsmanager");

    localstack.start();

    URI secretsManagerEndpoint = localstack.getEndpoint();

    try {
      secretsManagerClient =
          SecretsManagerClient.builder()
              .endpointOverride(secretsManagerEndpoint)
              .region(Region.of(localstack.getRegion()))
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create(
                          localstack.getAccessKey(), localstack.getSecretKey())))
              .build();

      return ImmutableMap.<String, String>builder()
          .put("quarkus.secretsmanager.endpoint-override", secretsManagerEndpoint.toString())
          .put("quarkus.secretsmanager.aws.region", localstack.getRegion())
          .put("quarkus.secretsmanager.aws.credentials.type", "static")
          .put(
              "quarkus.secretsmanager.aws.credentials.static-provider.access-key-id",
              localstack.getAccessKey())
          .put(
              "quarkus.secretsmanager.aws.credentials.static-provider.secret-access-key",
              localstack.getSecretKey())
          .put("nessie.secrets.type", ExternalSecretsManagerType.AMAZON)
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
    try {
      if (secretsManagerClient != null) {
        secretsManagerClient.close();
      }
    } finally {
      secretsManagerClient = null;

      if (localstack != null) {
        try {
          localstack.stop();
        } finally {
          localstack = null;
        }
      }
    }
  }

  void updateSecrets(Map<String, Secret> secrets, Map<String, SecretType> secretTypes) {
    secrets.forEach(
        (sec, secret) -> {
          String string;
          if (secret instanceof KeySecret key) {
            string = key.key();
          } else if (secret instanceof BasicCredentials basic) {
            string = format("{\"name\": \"%s\", \"secret\": \"%s\"}", basic.name(), basic.secret());
          } else if (secret instanceof TokenSecret token) {
            string =
                token.expiresAt().isPresent()
                    ? format(
                        "{\"token\": \"%s\", \"expiresAt\": \"%s\"}",
                        token.token(), token.expiresAt().get())
                    : format("{\"token\": \"%s\"}", token.token());
          } else {
            throw new RuntimeException("Unsupported secret type: " + secret);
          }

          secretsManagerClient.createSecret(
              CreateSecretRequest.builder()
                  .name(NESSIE_SECRETS_PATH + '.' + sec)
                  .secretString(string)
                  .build());
        });
  }

  @Override
  public void inject(TestInjector testInjector) {
    testInjector.injectIntoFields(
        localstack.getEndpoint(),
        new AnnotatedAndMatchesType(AwsSecretsManagerEndpoint.class, URI.class));
    testInjector.injectIntoFields(
        localstack.getRegion(),
        new AnnotatedAndMatchesType(AwsSecretsManagerRegion.class, String.class));
    testInjector.injectIntoFields(
        localstack.getAccessKey(),
        new AnnotatedAndMatchesType(AwsSecretsManagerAccessKey.class, String.class));
    testInjector.injectIntoFields(
        localstack.getSecretKey(),
        new AnnotatedAndMatchesType(AwsSecretsManagerSecretAccessKey.class, String.class));
    testInjector.injectIntoFields(
        (SecretsUpdateHandler) this::updateSecrets,
        new AnnotatedAndMatchesType(SecretsUpdater.class, SecretsUpdateHandler.class));
  }
}

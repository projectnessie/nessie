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
package org.projectnessie.catalog.files.s3;

import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;
import static org.projectnessie.catalog.secrets.UnsafePlainTextSecretsManager.unsafePlainTextSecretsProvider;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.catalog.files.AbstractClients;
import org.projectnessie.catalog.files.api.BackendExceptionMapper;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.files.config.ImmutableS3NamedBucketOptions;
import org.projectnessie.catalog.files.config.ImmutableS3Options;
import org.projectnessie.catalog.files.config.ImmutableSecretStore;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.secrets.ResolvingSecretsProvider;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.http.SdkHttpClient;

public class TestS3Clients extends AbstractClients {

  private static SdkHttpClient sdkHttpClient;

  @BeforeAll
  static void createHttpClient() {
    S3Config s3Config = S3Config.builder().build();
    sdkHttpClient =
        S3Clients.apacheHttpClient(
            s3Config,
            ResolvingSecretsProvider.builder()
                .putSecretsManager("plain", unsafePlainTextSecretsProvider(Map.of()))
                .build());
  }

  @AfterAll
  static void closeHttpClient() {
    if (sdkHttpClient != null) {
      sdkHttpClient.close();
    }
  }

  @Override
  protected ObjectIO buildObjectIO(
      ObjectStorageMock.MockServer server1, ObjectStorageMock.MockServer server2) {

    String s3accessKeyName1 = "urn:nessie-secret:plain:s3-access-key1";
    String s3accessKeyName2 = "s3-access-key2";
    URI s3accessKeyUri1 = URI.create("urn:nessie-secret:plain:" + s3accessKeyName1);
    URI s3accessKeyUri2 = URI.create("urn:nessie-secret:plain:" + s3accessKeyName2);
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager(
                "plain",
                unsafePlainTextSecretsProvider(
                    Map.of(
                        s3accessKeyName1,
                        basicCredentials("ak1", "sak1").asMap(),
                        s3accessKeyName2,
                        basicCredentials("ak2", "sak2").asMap())))
            .build();

    ImmutableS3Options.Builder s3options =
        ImmutableS3Options.builder()
            .putBucket(
                BUCKET_1,
                ImmutableS3NamedBucketOptions.builder()
                    .endpoint(server1.getS3BaseUri())
                    .region("us-west-1")
                    .accessKey(s3accessKeyUri1)
                    .accessPoint(BUCKET_1)
                    .build());
    if (server2 != null) {
      s3options.putBucket(
          BUCKET_2,
          ImmutableS3NamedBucketOptions.builder()
              .endpoint(server2.getS3BaseUri())
              .region("eu-central-2")
              .accessKey(s3accessKeyUri2)
              .build());
    }

    S3ClientSupplier supplier =
        new S3ClientSupplier(sdkHttpClient, s3options.build(), null, secretsProvider);
    return new S3ObjectIO(supplier, null);
  }

  @Override
  protected StorageUri buildURI(String bucket, String key) {
    return StorageUri.of(String.format("s3://%s/%s", bucket, key));
  }

  @Override
  protected BackendExceptionMapper.Builder addExceptionHandlers(
      BackendExceptionMapper.Builder builder) {
    return builder.addAnalyzer(S3ExceptionMapper.INSTANCE);
  }

  @Test
  public void invalidTrustStore(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("my.trust.store");

    String password = "very_secret";
    char[] passwordChars = password.toCharArray();

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    try (OutputStream out = Files.newOutputStream(file)) {
      ks.load(null, null);
      ks.store(out, passwordChars);
    }

    String correct = "correct-password";
    String wrong = "wrong_password";
    URI correctUri = URI.create("urn:nessie-secret:plain:" + correct);
    URI wrongUri = URI.create("urn:nessie-secret:plain:" + wrong);
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager(
                "plain",
                unsafePlainTextSecretsProvider(
                    Map.of(
                        correct, keySecret(password).asMap(),
                        wrong, keySecret("wrong_password").asMap())))
            .build();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStore(ImmutableSecretStore.builder().path(file).build())
                            .build(),
                        ResolvingSecretsProvider.builder()
                            .putSecretsManager("plain", unsafePlainTextSecretsProvider(Map.of()))
                            .build())
                    .close())
        .withMessage("No trust store type");
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStore(
                                ImmutableSecretStore.builder()
                                    .path(tempDir.resolve("not.there"))
                                    .type("jks")
                                    .password(correctUri)
                                    .build())
                            .build(),
                        secretsProvider)
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(NoSuchFileException.class);
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStore(
                                ImmutableSecretStore.builder()
                                    .path(file)
                                    .type("jks")
                                    .password(wrongUri)
                                    .build())
                            .build(),
                        secretsProvider)
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class);
    soft.assertThatCode(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStore(
                                ImmutableSecretStore.builder()
                                    .path(file)
                                    .type("jks")
                                    .password(correctUri)
                                    .build())
                            .build(),
                        secretsProvider)
                    .close())
        .doesNotThrowAnyException();
  }

  @Test
  public void invalidKeyStore(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("my.key.store");

    String password = "very_secret";
    char[] passwordChars = password.toCharArray();

    KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
    try (OutputStream out = Files.newOutputStream(file)) {
      ks.load(null, null);
      ks.store(out, passwordChars);
    }

    String correct = "correct-password";
    String wrong = "wrong_password";
    URI correctUri = URI.create("urn:nessie-secret:plain:" + correct);
    URI wrongUri = URI.create("urn:nessie-secret:plain:" + wrong);
    SecretsProvider secretsProvider =
        ResolvingSecretsProvider.builder()
            .putSecretsManager(
                "plain",
                unsafePlainTextSecretsProvider(
                    Map.of(
                        correct, keySecret(password).asMap(),
                        wrong, keySecret("wrong_password").asMap())))
            .build();

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStore(ImmutableSecretStore.builder().path(file).build())
                            .build(),
                        secretsProvider)
                    .close())
        .withMessage("No key store type");
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStore(
                                ImmutableSecretStore.builder()
                                    .path(tempDir.resolve("not.there"))
                                    .type("jks")
                                    .password(correctUri)
                                    .build())
                            .build(),
                        secretsProvider)
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(NoSuchFileException.class);
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStore(
                                ImmutableSecretStore.builder()
                                    .path(file)
                                    .type("jks")
                                    .password(wrongUri)
                                    .build())
                            .build(),
                        secretsProvider)
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class);
    soft.assertThatCode(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStore(
                                ImmutableSecretStore.builder()
                                    .path(file)
                                    .type("jks")
                                    .password(correctUri)
                                    .build())
                            .build(),
                        secretsProvider)
                    .close())
        .doesNotThrowAnyException();
  }
}

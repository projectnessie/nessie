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

import static java.util.function.Function.identity;
import static org.projectnessie.catalog.secrets.BasicCredentials.basicCredentials;
import static org.projectnessie.catalog.secrets.KeySecret.keySecret;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.catalog.files.AbstractClients;
import org.projectnessie.catalog.files.api.ObjectIO;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.objectstoragemock.ObjectStorageMock;
import org.projectnessie.storage.uri.StorageUri;
import software.amazon.awssdk.http.SdkHttpClient;

public class TestS3Clients extends AbstractClients {

  private static SdkHttpClient sdkHttpClient;

  @BeforeAll
  static void createHttpClient() {
    S3Config s3Config = S3Config.builder().build();
    sdkHttpClient = S3Clients.apacheHttpClient(s3Config, new SecretsProvider(names -> Map.of()));
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

    S3ProgrammaticOptions.Builder s3options =
        S3ProgrammaticOptions.builder()
            .putBuckets(
                BUCKET_1,
                S3ProgrammaticOptions.S3PerBucketOptions.builder()
                    .endpoint(server1.getS3BaseUri())
                    .region("us-west-1")
                    .accessKey(basicCredentials("ak1", "sak1"))
                    .accessPoint(BUCKET_1)
                    .build());
    if (server2 != null) {
      s3options.putBuckets(
          BUCKET_2,
          S3ProgrammaticOptions.S3PerBucketOptions.builder()
              .endpoint(server2.getS3BaseUri())
              .region("eu-central-2")
              .accessKey(basicCredentials("ak2", "sak2"))
              .build());
    }

    S3ClientSupplier supplier =
        new S3ClientSupplier(
            sdkHttpClient,
            S3Config.builder().build(),
            s3options.build(),
            new SecretsProvider(
                names ->
                    names.stream()
                        .collect(Collectors.toMap(identity(), k -> Map.of("secret", "secret")))),
            null);
    return new S3ObjectIO(supplier);
  }

  @Override
  protected StorageUri buildURI(String bucket, String key) {
    return StorageUri.of(String.format("s3://%s/%s", bucket, key));
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

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder().trustStorePath(file).build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .withMessage("No trust store type");
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStorePath(tempDir.resolve("not.there"))
                            .trustStoreType("jks")
                            .trustStorePassword(keySecret(password))
                            .build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(NoSuchFileException.class);
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStorePath(file)
                            .trustStoreType("jks")
                            .trustStorePassword(keySecret("wrong_password"))
                            .build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class);
    soft.assertThatCode(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .trustStorePath(file)
                            .trustStoreType("jks")
                            .trustStorePassword(keySecret(password))
                            .build(),
                        new SecretsProvider(names -> Map.of()))
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

    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder().keyStorePath(file).build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .withMessage("No key store type");
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStorePath(tempDir.resolve("not.there"))
                            .keyStoreType("jks")
                            .keyStorePassword(keySecret(password))
                            .build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(NoSuchFileException.class);
    soft.assertThatThrownBy(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStorePath(file)
                            .keyStoreType("jks")
                            .keyStorePassword(keySecret("wrong_password"))
                            .build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .isInstanceOf(RuntimeException.class)
        .cause()
        .isInstanceOf(IOException.class);
    soft.assertThatCode(
            () ->
                S3Clients.apacheHttpClient(
                        S3Config.builder()
                            .keyStorePath(file)
                            .keyStoreType("jks")
                            .keyStorePassword(keySecret(password))
                            .build(),
                        new SecretsProvider(names -> Map.of()))
                    .close())
        .doesNotThrowAnyException();
  }
}

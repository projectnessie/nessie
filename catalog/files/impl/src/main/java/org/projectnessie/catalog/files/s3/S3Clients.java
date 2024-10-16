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

import static org.projectnessie.catalog.files.s3.S3Utils.newCredentialsProvider;
import static org.projectnessie.catalog.secrets.SecretType.KEY;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.projectnessie.catalog.files.config.S3BucketOptions;
import org.projectnessie.catalog.files.config.S3Config;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.TlsTrustManagersProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.internal.http.AbstractFileStoreTlsKeyManagersProvider;
import software.amazon.awssdk.utils.AttributeMap;
import software.amazon.awssdk.utils.Validate;

public class S3Clients {

  /** Builds an SDK Http client based on the Apache Http client. */
  public static SdkHttpClient apacheHttpClient(S3Config s3Config, SecretsProvider secretsProvider) {
    ApacheHttpClient.Builder httpClient = ApacheHttpClient.builder();
    s3Config
        .http()
        .ifPresent(
            http -> {
              http.maxHttpConnections().ifPresent(httpClient::maxConnections);
              http.readTimeout().ifPresent(httpClient::socketTimeout);
              http.connectTimeout().ifPresent(httpClient::connectionTimeout);
              http.connectionAcquisitionTimeout()
                  .ifPresent(httpClient::connectionAcquisitionTimeout);
              http.connectionMaxIdleTime().ifPresent(httpClient::connectionMaxIdleTime);
              http.connectionTimeToLive().ifPresent(httpClient::connectionTimeToLive);
              http.expectContinueEnabled().ifPresent(httpClient::expectContinueEnabled);
            });
    s3Config
        .trustStore()
        .ifPresent(
            trustStore ->
                trustStore
                    .path()
                    .ifPresent(
                        p -> {
                          char[] password =
                              trustStore
                                  .password()
                                  .flatMap(
                                      secretName ->
                                          secretsProvider.getSecret(
                                              secretName, KEY, KeySecret.class))
                                  .map(KeySecret::key)
                                  .map(String::toCharArray)
                                  .orElse(null);

                          httpClient.tlsTrustManagersProvider(
                              new FileStoreTlsTrustManagersProvider(
                                  p,
                                  trustStore
                                      .type()
                                      .orElseThrow(
                                          () ->
                                              new IllegalArgumentException("No trust store type")),
                                  password));
                        }));
    s3Config
        .keyStore()
        .ifPresent(
            keyStore ->
                keyStore
                    .path()
                    .ifPresent(
                        p -> {
                          char[] password =
                              keyStore
                                  .password()
                                  .flatMap(
                                      secretName ->
                                          secretsProvider.getSecret(
                                              secretName, KEY, KeySecret.class))
                                  .map(KeySecret::key)
                                  .map(String::toCharArray)
                                  .orElse(null);

                          httpClient.tlsKeyManagersProvider(
                              new FileStoreTlsKeyManagersProvider(
                                  p,
                                  keyStore
                                      .type()
                                      .orElseThrow(
                                          () -> new IllegalArgumentException("No key store type")),
                                  password));
                        }));
    AttributeMap.Builder options = AttributeMap.builder();
    s3Config.trustAllCertificates().ifPresent(v -> options.put(TRUST_ALL_CERTIFICATES, v));
    return httpClient.buildWithDefaults(options.build());
  }

  public static AwsCredentialsProvider serverCredentialsProvider(
      S3BucketOptions bucketOptions, S3Sessions sessions, SecretsProvider secretsProvider) {
    return bucketOptions.getEnabledServerIam().isPresent()
        ? sessions.assumeRoleForServer(bucketOptions)
        : newCredentialsProvider(bucketOptions.effectiveAuthType(), bucketOptions, secretsProvider);
  }

  private static final class FileStoreTlsTrustManagersProvider implements TlsTrustManagersProvider {
    private final Path path;
    private final String type;
    private final char[] password;

    FileStoreTlsTrustManagersProvider(Path path, String type, char[] password) {
      this.path = path;
      this.type = type;
      this.password = password;
    }

    @Override
    public TrustManager[] trustManagers() {
      try (InputStream storeInputStream = Files.newInputStream(path)) {
        KeyStore keyStore = KeyStore.getInstance(type);
        keyStore.load(storeInputStream, password);
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(keyStore);
        return tmf.getTrustManagers();
      } catch (KeyStoreException
          | CertificateException
          | NoSuchAlgorithmException
          | IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static final class FileStoreTlsKeyManagersProvider
      extends AbstractFileStoreTlsKeyManagersProvider {

    private final Path storePath;
    private final String storeType;
    private final char[] password;

    FileStoreTlsKeyManagersProvider(Path storePath, String storeType, char[] password) {
      this.storePath = Validate.paramNotNull(storePath, "storePath");
      this.storeType = Validate.paramNotBlank(storeType, "storeType");
      this.password = password;
    }

    @Override
    public KeyManager[] keyManagers() {
      try {
        return createKeyManagers(storePath, storeType, password);
      } catch (CertificateException
          | UnrecoverableKeyException
          | IOException
          | KeyStoreException
          | NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

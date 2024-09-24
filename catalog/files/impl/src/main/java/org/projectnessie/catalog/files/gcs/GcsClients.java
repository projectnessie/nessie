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
package org.projectnessie.catalog.files.gcs;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Date;
import java.util.function.Function;
import java.util.function.Supplier;
import org.projectnessie.catalog.files.config.GcsBucketOptions;
import org.projectnessie.catalog.files.config.GcsBucketOptions.GcsAuthType;
import org.projectnessie.catalog.files.config.GcsConfig;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.catalog.secrets.TokenSecret;

public final class GcsClients {
  private GcsClients() {}

  public static Storage buildStorage(
      GcsConfig gcsConfig,
      GcsBucketOptions bucketOptions,
      HttpTransportFactory transportFactory,
      SecretsProvider secretsProvider) {
    HttpTransportOptions.Builder transportOptions =
        HttpTransportOptions.newBuilder().setHttpTransportFactory(transportFactory);
    gcsConfig
        .connectTimeout()
        .ifPresent(d -> transportOptions.setConnectTimeout((int) d.toMillis()));
    gcsConfig.readTimeout().ifPresent(d -> transportOptions.setReadTimeout((int) d.toMillis()));

    StorageOptions.Builder builder =
        StorageOptions.http()
            .setCredentials(buildCredentials(bucketOptions, transportFactory, secretsProvider))
            .setTransportOptions(transportOptions.build());
    bucketOptions.projectId().ifPresent(builder::setProjectId);
    bucketOptions.quotaProjectId().ifPresent(builder::setQuotaProjectId);
    bucketOptions.host().map(URI::toString).ifPresent(builder::setHost);
    bucketOptions.clientLibToken().ifPresent(builder::setClientLibToken);
    builder.setRetrySettings(buildRetrySettings(gcsConfig));
    // TODO ??
    // bucketOptions.buildStorageRetryStrategy().ifPresent(builder::setStorageRetryStrategy);

    return builder.build().getService();
  }

  static RetrySettings buildRetrySettings(GcsConfig gcsConfig) {
    Function<Duration, org.threeten.bp.Duration> duration =
        d -> org.threeten.bp.Duration.ofMillis(d.toMillis());

    RetrySettings.Builder retry = RetrySettings.newBuilder();
    gcsConfig.maxAttempts().ifPresent(retry::setMaxAttempts);
    gcsConfig.logicalTimeout().map(duration).ifPresent(retry::setLogicalTimeout);
    gcsConfig.totalTimeout().map(duration).ifPresent(retry::setTotalTimeout);

    gcsConfig.initialRetryDelay().map(duration).ifPresent(retry::setInitialRetryDelay);
    gcsConfig.maxRetryDelay().map(duration).ifPresent(retry::setMaxRetryDelay);
    gcsConfig.retryDelayMultiplier().ifPresent(retry::setRetryDelayMultiplier);

    gcsConfig.initialRpcTimeout().map(duration).ifPresent(retry::setInitialRpcTimeout);
    gcsConfig.maxRpcTimeout().map(duration).ifPresent(retry::setMaxRpcTimeout);
    gcsConfig.rpcTimeoutMultiplier().ifPresent(retry::setRpcTimeoutMultiplier);

    return retry.build();
  }

  public static HttpTransportFactory buildSharedHttpTransportFactory() {
    // Uses the java.net.HttpURLConnection stuff...
    NetHttpTransport.Builder httpTransport = new NetHttpTransport.Builder();
    return new SharedHttpTransportFactory(httpTransport::build);
  }

  static final class SharedHttpTransportFactory implements HttpTransportFactory {
    private final Supplier<HttpTransport> delegate;
    private volatile HttpTransport httpTransport;

    SharedHttpTransportFactory(Supplier<HttpTransport> delegate) {
      this.delegate = delegate;
    }

    @Override
    public HttpTransport create() {
      if (httpTransport == null) {
        synchronized (this) {
          if (httpTransport == null) {
            httpTransport = delegate.get();
          }
        }
      }
      return httpTransport;
    }
  }

  static Credentials buildCredentials(
      GcsBucketOptions bucketOptions,
      HttpTransportFactory transportFactory,
      SecretsProvider secretsProvider) {
    GcsAuthType authType = bucketOptions.effectiveAuthType();
    switch (authType) {
      case NONE:
        return NoCredentials.getInstance();
      case USER:
        try {
          URI secretName =
              bucketOptions
                  .authCredentialsJson()
                  .orElseThrow(() -> new IllegalStateException("auth-credentials-json missing"));

          return UserCredentials.fromStream(
              new ByteArrayInputStream(
                  secretsProvider
                      .getSecret(secretName, SecretType.KEY, KeySecret.class)
                      .orElseThrow(() -> new IllegalStateException("auth-credentials-json missing"))
                      .key()
                      .getBytes(UTF_8)),
              transportFactory);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      case SERVICE_ACCOUNT:
        try {
          URI secretName =
              bucketOptions
                  .authCredentialsJson()
                  .orElseThrow(() -> new IllegalStateException("auth-credentials-json missing"));

          return ServiceAccountCredentials.fromStream(
              new ByteArrayInputStream(
                  secretsProvider
                      .getSecret(secretName, SecretType.KEY, KeySecret.class)
                      .orElseThrow(() -> new IllegalStateException("auth-credentials-json missing"))
                      .key()
                      .getBytes(UTF_8)),
              transportFactory);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      case ACCESS_TOKEN:
        {
          URI secretName =
              bucketOptions
                  .oauth2Token()
                  .orElseThrow(() -> new IllegalStateException("oauth2-token missing"));

          TokenSecret oauth2token =
              secretsProvider
                  .getSecret(secretName, SecretType.EXPIRING_TOKEN, TokenSecret.class)
                  .orElseThrow(() -> new IllegalStateException("oauth2-token missing"));
          AccessToken accessToken =
              new AccessToken(
                  oauth2token.token(),
                  oauth2token.expiresAt().map(i -> new Date(i.toEpochMilli())).orElse(null));
          return OAuth2Credentials.create(accessToken);
        }
      case APPLICATION_DEFAULT:
        try {
          return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
          throw new IllegalArgumentException("Unable to load default credentials", e);
        }
      default:
        throw new IllegalArgumentException("Unsupported auth type " + authType);
    }
  }
}

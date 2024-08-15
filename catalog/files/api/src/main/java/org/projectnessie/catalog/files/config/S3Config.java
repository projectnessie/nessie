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
package org.projectnessie.catalog.files.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;

/**
 * Configuration for S3 compatible object stores.
 *
 * <p>Default settings to be applied to all buckets can be set in the {@code default-options} group.
 * Specific settings for each bucket can be specified via the {@code buckets} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the AWSSDK Java
 * client.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableS3Config.class)
@JsonDeserialize(as = ImmutableS3Config.class)
@ConfigMapping(prefix = "nessie.catalog.service.s3")
public interface S3Config {
  /** Override the default maximum number of pooled connections. */
  @ConfigItem(section = "transport")
  @WithName("http.max-http-connections")
  OptionalInt maxHttpConnections();

  /** Override the default connection read timeout. */
  @ConfigItem(section = "transport")
  @WithName("http.read-timeout")
  Optional<Duration> readTimeout();

  /** Override the default TCP connect timeout. */
  @ConfigItem(section = "transport")
  @WithName("http.connect-timeout")
  Optional<Duration> connectTimeout();

  /**
   * Override default connection acquisition timeout. This is the time a request will wait for a
   * connection from the pool.
   */
  @ConfigItem(section = "transport")
  @WithName("http.connection-acquisition-timeout")
  Optional<Duration> connectionAcquisitionTimeout();

  /** Override default max idle time of a pooled connection. */
  @ConfigItem(section = "transport")
  @WithName("http.connection-max-idle-time")
  Optional<Duration> connectionMaxIdleTime();

  /** Override default time-time of a pooled connection. */
  @ConfigItem(section = "transport")
  @WithName("http.connection-time-to-live")
  Optional<Duration> connectionTimeToLive();

  /** Override default behavior whether to expect an HTTP/100-Continue. */
  @ConfigItem(section = "transport")
  @WithName("http.expect-continue-enabled")
  Optional<Boolean> expectContinueEnabled();

  /**
   * Instruct the S3 HTTP client to accept all SSL certificates, if set to {@code true}. Enabling
   * this option is dangerous, it is strongly recommended to leave this option unset or {@code
   * false}.
   */
  @ConfigItem(section = "transport")
  @WithName("trust-all-certificates")
  Optional<Boolean> trustAllCertificates();

  /**
   * Override to set the file path to a custom SSL trust store. {@code
   * nessie.catalog.service.s3.trust-store.type} and {@code
   * nessie.catalog.service.s3.trust-store.password} must be supplied as well when providing a
   * custom trust store.
   *
   * <p>When running in k8s or Docker, the path is local within the pod/container and must be
   * explicitly mounted.
   */
  @ConfigItem(section = "transport")
  @WithName("trust-store.path")
  Optional<Path> trustStorePath();

  /**
   * Override to set the type of the custom SSL trust store specified in {@code
   * nessie.catalog.service.s3.trust-store.path}.
   *
   * <p>Supported types include {@code JKS}, {@code PKCS12}, and all key store types supported by
   * Java 17.
   */
  @ConfigItem(section = "transport")
  @WithName("trust-store.type")
  Optional<String> trustStoreType();

  /**
   * Name of the key-secret containing the password for the custom SSL trust store specified in
   * {@code nessie.catalog.service.s3.trust-store.path}.
   */
  @ConfigItem(section = "transport")
  @WithName("trust-store.password")
  Optional<URI> trustStorePassword();

  /**
   * Override to set the file path to a custom SSL key store. {@code
   * nessie.catalog.service.s3.key-store.type} and {@code
   * nessie.catalog.service.s3.key-store.password} must be supplied as well when providing a custom
   * key store.
   *
   * <p>When running in k8s or Docker, the path is local within the pod/container and must be
   * explicitly mounted.
   */
  @ConfigItem(section = "transport")
  @WithName("key-store.path")
  Optional<Path> keyStorePath();

  /**
   * Override to set the type of the custom SSL key store specified in {@code
   * nessie.catalog.service.s3.key-store.path}.
   *
   * <p>Supported types include {@code JKS}, {@code PKCS12}, and all key store types supported by
   * Java 17.
   */
  @ConfigItem(section = "transport")
  @WithName("key-store.type")
  Optional<String> keyStoreType();

  /**
   * Name of the secret containing the password for the custom SSL key store specified in {@code
   * nessie.catalog.service.s3.key-store.path}.
   */
  @ConfigItem(section = "transport")
  @WithName("key-store.password")
  Optional<URI> keyStorePassword();

  static Builder builder() {
    return ImmutableS3Config.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(S3Config instance);

    @CanIgnoreReturnValue
    Builder maxHttpConnections(int maxHttpConnections);

    @CanIgnoreReturnValue
    Builder readTimeout(Duration readTimeout);

    @CanIgnoreReturnValue
    Builder connectTimeout(Duration connectTimeout);

    @CanIgnoreReturnValue
    Builder connectionAcquisitionTimeout(Duration connectionAcquisitionTimeout);

    @CanIgnoreReturnValue
    Builder connectionMaxIdleTime(Duration connectionMaxIdleTime);

    @CanIgnoreReturnValue
    Builder connectionTimeToLive(Duration connectionTimeToLive);

    @CanIgnoreReturnValue
    Builder expectContinueEnabled(boolean expectContinueEnabled);

    @CanIgnoreReturnValue
    Builder trustStorePath(Path trustStorePath);

    @CanIgnoreReturnValue
    Builder trustStoreType(String trustStoreType);

    @CanIgnoreReturnValue
    Builder trustStorePassword(URI trustStorePassword);

    @CanIgnoreReturnValue
    Builder keyStorePath(Path keyStorePath);

    @CanIgnoreReturnValue
    Builder keyStoreType(String keyStoreType);

    @CanIgnoreReturnValue
    Builder keyStorePassword(URI keyStorePassword);

    S3Config build();
  }
}

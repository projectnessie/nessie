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
package org.projectnessie.quarkus.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.projectnessie.catalog.files.s3.S3Config;
import org.projectnessie.catalog.files.s3.S3Options;
import org.projectnessie.catalog.secrets.KeySecret;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

/**
 * Configuration for S3 compatible object stores.
 *
 * <p>Default settings to be applied to all buckets can be set in the {@code default-options} group.
 * Specific settings for each bucket can be specified via the {@code buckets} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the AWSSDK Java
 * client.
 */
@ConfigMapping(prefix = "nessie.catalog.service.s3")
public interface CatalogS3Config extends S3Config, S3Options<CatalogS3BucketConfig> {
  @WithName("throttled-retry-after")
  @WithDefault("PT10S")
  @Override
  Optional<Duration> retryAfter();

  @WithName("http.expect-continue-enabled")
  @Override
  Optional<Boolean> expectContinueEnabled();

  @WithName("http.connection-time-to-live")
  @Override
  Optional<Duration> connectionTimeToLive();

  @WithName("http.connection-max-idle-time")
  @Override
  Optional<Duration> connectionMaxIdleTime();

  @WithName("http.connection-acquisition-timeout")
  @Override
  Optional<Duration> connectionAcquisitionTimeout();

  @WithName("http.connect-timeout")
  @Override
  Optional<Duration> connectTimeout();

  @WithName("http.read-timeout")
  @Override
  Optional<Duration> readTimeout();

  @WithName("http.max-http-connections")
  @Override
  OptionalInt maxHttpConnections();

  @WithName("sts.session-grace-period")
  @Override
  Optional<Duration> sessionCredentialRefreshGracePeriod();

  @WithName("sts.session-cache-max-size")
  @Override
  OptionalInt sessionCredentialCacheMaxEntries();

  @WithName("sts.clients-cache-max-size")
  @Override
  OptionalInt stsClientsCacheMaxEntries();

  @WithName("trust-all-certificates")
  @Override
  Optional<Boolean> trustAllCertificates();

  @WithName("trust-store.path")
  @Override
  Optional<Path> trustStorePath();

  @WithName("trust-store.type")
  @Override
  Optional<String> trustStoreType();

  @WithName("trust-store.password")
  @Override
  Optional<KeySecret> trustStorePassword();

  @WithName("key-store.path")
  @Override
  Optional<Path> keyStorePath();

  @WithName("key-store.type")
  @Override
  Optional<String> keyStoreType();

  @WithName("key-store.password")
  @Override
  Optional<KeySecret> keyStorePassword();

  @WithName("buckets")
  @ConfigPropertyName("bucket-name")
  @Override
  Map<String, CatalogS3BucketConfig> buckets();

  @Override
  Optional<CatalogS3BucketConfig> defaultOptions();
}

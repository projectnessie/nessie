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
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.catalog.files.gcs.GcsConfig;
import org.projectnessie.catalog.files.gcs.GcsOptions;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

/**
 * Configuration for Google Cloud Storage (GCS) object stores.
 *
 * <p>Contains the default settings to be applied to all buckets. Specific settings for each bucket
 * can be specified via the {@code buckets} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the Google Java SDK
 * client.
 */
@ConfigMapping(prefix = "nessie.catalog.service.gcs")
public interface CatalogGcsConfig extends GcsConfig, GcsOptions<CatalogGcsBucketConfig> {

  @Override
  @ConfigPropertyName("bucket-name")
  Map<String, CatalogGcsBucketConfig> buckets();

  @Override
  Optional<URI> host();

  @Override
  Optional<URI> externalHost();

  @Override
  Optional<String> projectId();

  @Override
  Optional<String> quotaProjectId();

  @Override
  Optional<String> clientLibToken();

  @Override
  Optional<GcsAuthType> authType();

  @Override
  Optional<String> authCredentialsJsonRef();

  @Override
  Optional<String> oauth2TokenRef();

  @Override
  Optional<Instant> oauth2TokenExpiresAt();

  @Override
  OptionalInt maxAttempts();

  @Override
  Optional<Duration> logicalTimeout();

  @Override
  Optional<Duration> totalTimeout();

  @Override
  Optional<Duration> initialRetryDelay();

  @Override
  Optional<Duration> maxRetryDelay();

  @Override
  OptionalDouble retryDelayMultiplier();

  @Override
  Optional<Duration> initialRpcTimeout();

  @Override
  Optional<Duration> maxRpcTimeout();

  @Override
  OptionalDouble rpcTimeoutMultiplier();

  @Override
  OptionalInt readChunkSize();

  @Override
  OptionalInt writeChunkSize();

  @Override
  OptionalInt deleteBatchSize();

  @Override
  Optional<String> encryptionKeyRef();

  @Override
  Optional<String> decryptionKeyRef();

  @Override
  Optional<String> userProject();

  @Override
  Optional<Duration> readTimeout();

  @Override
  Optional<Duration> connectTimeout();
}

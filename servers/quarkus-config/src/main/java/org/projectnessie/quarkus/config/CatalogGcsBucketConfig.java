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

import io.smallrye.config.WithConverter;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Optional;
import org.projectnessie.catalog.files.gcs.GcsBucketOptions;
import org.projectnessie.catalog.secrets.KeySecret;

public interface CatalogGcsBucketConfig extends GcsBucketOptions {

  @Override
  Optional<GcsAuthType> authType();

  @Override
  @WithConverter(KeySecretConverter.class)
  Optional<KeySecret> authCredentialsJson();

  @Override
  @WithConverter(KeySecretConverter.class)
  Optional<KeySecret> encryptionKey();

  @Override
  @WithConverter(KeySecretConverter.class)
  Optional<KeySecret> decryptionKey();

  @Override
  @WithName("downscoped-credentials.enable")
  Optional<Boolean> downscopedCredentialsEnable();

  @Override
  @WithName("downscoped-credentials.expiration-margin")
  Optional<Duration> downscopedCredentialsExpirationMargin();

  @Override
  @WithName("downscoped-credentials.refresh-margin")
  Optional<Duration> downscopedCredentialsRefreshMargin();
}

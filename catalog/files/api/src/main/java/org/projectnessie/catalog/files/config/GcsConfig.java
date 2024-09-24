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
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.immutables.NessieImmutable;

/** System level configuration for Google Cloud Storage (GCS) object stores. */
@NessieImmutable
@JsonSerialize(as = ImmutableGcsConfig.class)
@JsonDeserialize(as = ImmutableGcsConfig.class)
public interface GcsConfig {

  /** Override the default read timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> readTimeout();

  /** Override the default connection timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> connectTimeout();

  /** Override the default maximum number of attempts. */
  @ConfigItem(section = "transport")
  OptionalInt maxAttempts();

  /** Override the default logical request timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> logicalTimeout();

  /** Override the default total timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> totalTimeout();

  /** Override the default initial retry delay. */
  @ConfigItem(section = "transport")
  Optional<Duration> initialRetryDelay();

  /** Override the default maximum retry delay. */
  @ConfigItem(section = "transport")
  Optional<Duration> maxRetryDelay();

  /** Override the default retry delay multiplier. */
  @ConfigItem(section = "transport")
  OptionalDouble retryDelayMultiplier();

  /** Override the default initial RPC timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> initialRpcTimeout();

  /** Override the default maximum RPC timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> maxRpcTimeout();

  /** Override the default RPC timeout multiplier. */
  @ConfigItem(section = "transport")
  OptionalDouble rpcTimeoutMultiplier();
}

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableAdlsFileSystemOptions.class)
@JsonDeserialize(as = ImmutableAdlsFileSystemOptions.class)
public interface AdlsFileSystemOptions extends BucketOptions {

  Duration DELEGATION_KEY_DEFAULT_EXPIRY = Duration.ofDays(7).minus(1, ChronoUnit.SECONDS);
  Duration DELEGATION_SAS_DEFAULT_EXPIRY = Duration.ofHours(3);

  /** The authentication type to use. */
  Optional<AzureAuthType> authType();

  /**
   * Name of the basic-credentials secret containing the fully-qualified account name, e.g. {@code
   * "myaccount.dfs.core.windows.net"} and account key, configured using the {@code name} and {@code
   * secret} fields. If not specified, it will be queried via the configured credentials provider.
   */
  Optional<URI> account();

  /** Name of the key-secret containing the SAS token to access the ADLS file system. */
  Optional<URI> sasToken();

  Optional<AdlsUserDelegation> userDelegation();

  @Value.NonAttribute
  @JsonIgnore
  default AdlsUserDelegation effectiveUserDelegation() {
    return userDelegation().orElse(ImmutableAdlsUserDelegation.builder().build());
  }

  /**
   * Define a custom HTTP endpoint. In case clients need to use a different URI, use the {@code
   * .external-endpoint} setting.
   */
  Optional<String> endpoint();

  /** Define a custom HTTP endpoint, this value is used by clients. */
  Optional<String> externalEndpoint();

  /** Configure the retry strategy. */
  Optional<AdlsRetryStrategy> retryPolicy();

  /** Mandatory, if any {@code retry-policy} is configured. */
  Optional<Integer> maxRetries();

  /** Mandatory, if any {@code retry-policy} is configured. */
  Optional<Duration> tryTimeout();

  /** Mandatory, if any {@code retry-policy} is configured. */
  Optional<Duration> retryDelay();

  /** Mandatory, if {@code EXPONENTIAL_BACKOFF} is configured. */
  Optional<Duration> maxRetryDelay();

  enum AdlsRetryStrategy {
    /** Same as not configuring a retry strategy. */
    NONE,
    EXPONENTIAL_BACKOFF,
    FIXED_DELAY,
  }

  enum AzureAuthType {
    NONE,
    STORAGE_SHARED_KEY,
    SAS_TOKEN,
    APPLICATION_DEFAULT
  }

  @Value.NonAttribute
  @JsonIgnore
  default AdlsFileSystemOptions deepClone() {
    ImmutableAdlsFileSystemOptions.Builder b = ImmutableAdlsFileSystemOptions.builder().from(this);
    userDelegation().ifPresent(v -> b.userDelegation(ImmutableAdlsUserDelegation.copyOf(v)));
    return b.build();
  }
}

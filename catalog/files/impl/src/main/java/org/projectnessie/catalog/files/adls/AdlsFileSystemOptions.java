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
package org.projectnessie.catalog.files.adls;

import java.time.Duration;
import java.util.Optional;

public interface AdlsFileSystemOptions {

  /** Fully-qualified account name, e.g. {@code "myaccount.dfs.core.windows.net"}. */
  Optional<String> accountNameRef();

  /** Account key to access the ADLS file system. */
  Optional<String> accountKeyRef();

  /**
   * SAS token <em>reference</em> to access the ADLS file system, the actual secret value is defined
   * as secrets via {@code nessie.catalog.secrets.}<em>{@code secret-ref}</em>.
   */
  Optional<String> sasTokenRef();

  /**
   * Define a custom HTTP endpoint. In case clients need to use a different URI, use the {@code
   * .external-endpoint} setting.
   */
  Optional<String> endpoint();

  /** Define a custom HTTP endpoint, this value is used by clients. */
  Optional<String> externalEndpoint();

  /** Configure the retry strategy. */
  Optional<AdlsRetryStrategy> retryPolicy();

  /** Mandatory, if any {@link AdlsRetryStrategy} is configured. */
  Optional<Integer> maxRetries();

  /** Mandatory, if any {@link AdlsRetryStrategy} is configured. */
  Optional<Duration> tryTimeout();

  /** Mandatory, if any {@link AdlsRetryStrategy} is configured. */
  Optional<Duration> retryDelay();

  /** Mandatory, if {@link AdlsRetryStrategy#EXPONENTIAL_BACKOFF} is configured. */
  Optional<Duration> maxRetryDelay();

  enum AdlsRetryStrategy {
    /** Same as not configuring a retry strategy. */
    NONE,
    EXPONENTIAL_BACKOFF,
    FIXED_DELAY,
  }
}

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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;

@Value.Immutable
public interface S3Config {
  /** Override the default maximum number of pooled connections. */
  OptionalInt maxHttpConnections();

  /** Override the default connection read timeout. */
  Optional<Duration> readTimeout();

  /** Override the default TCP connect timeout. */
  Optional<Duration> connectTimeout();

  /**
   * Override default connection acquisition timeout. This is the time a request will wait for a
   * connection from the pool.
   */
  Optional<Duration> connectionAcquisitionTimeout();

  /** Override default max idle time of a pooled connection. */
  Optional<Duration> connectionMaxIdleTime();

  /** Override default time-time of a pooled connection. */
  Optional<Duration> connectionTimeToLive();

  /** Override default behavior whether to expect an HTTP/100-Continue. */
  Optional<Boolean> expectContinueEnabled();

  /**
   * Interval after which a request is retried when S3 response with some "retry later" response.
   */
  Optional<Duration> retryAfter();

  static Builder builder() {
    return ImmutableS3Config.builder();
  }

  interface Builder {
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
    Builder retryAfter(Duration retryAfter);

    S3Config build();
  }
}

/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.bigtable;

import com.google.api.gax.retrying.RetrySettings;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

/**
 * Settings used to create and configure BigTable clients (data and table admin).
 *
 * @see BigTableClientsFactory
 * @see RetrySettings
 */
public interface BigTableClientsConfig {

  /** Sets the instance-id to be used with Google BigTable. */
  default String instanceId() {
    return "nessie";
  }

  /** Sets the profile-id to be used with Google BigTable. */
  Optional<String> appProfileId();

  /** Google BigTable quote project ID (optional). */
  Optional<String> quotaProjectId();

  /** Google BigTable endpoint (if not default). */
  Optional<String> endpoint();

  /** Google BigTable MTLS endpoint (if not default). */
  Optional<String> mtlsEndpoint();

  /** When using the BigTable emulator, used to configure the host. */
  Optional<String> emulatorHost();

  /** When using the BigTable emulator, used to configure the port. */
  default int emulatorPort() {
    return 8086;
  }

  /**
   * Was a setting for Google BigTable JWT audience mappings, but it is actually a no-op.
   *
   * @deprecated Audience is always set to bigtable service name.
   */
  @ConfigPropertyName("mapping")
  @Deprecated(forRemoval = true)
  Map<String, String> jwtAudienceMapping();

  /** Initial retry delay. */
  Optional<Duration> initialRetryDelay();

  /** Max retry-delay. */
  Optional<Duration> maxRetryDelay();

  OptionalDouble retryDelayMultiplier();

  /** Maximum number of attempts for each Bigtable API call (including retries). */
  OptionalInt maxAttempts();

  /** Initial RPC timeout. */
  Optional<Duration> initialRpcTimeout();

  Optional<Duration> maxRpcTimeout();

  OptionalDouble rpcTimeoutMultiplier();

  /** Total timeout (including retries) for Bigtable API calls. */
  Optional<Duration> totalTimeout();

  /**
   * Initial number of gRPC channels. Refer to Google docs for details.
   *
   * <p>When unset, the value tuned by the Bigtable client library is used.
   *
   * <p>Channel pools resize at most once per minute and by at most two channels at a time. Dynamic
   * resizing therefore cannot react to a burst shorter than a minute, so this setting and {@link
   * #minChannelCount()} are what actually determine burst headroom.
   *
   * <p>When {@link #minChannelCount()} or {@link #maxChannelCount()} is configured such that the
   * inherited value would fall outside that range, the inherited value is adjusted to fit.
   */
  OptionalInt initialChannelCount();

  /**
   * Minimum number of gRPC channels. Refer to Google docs for details.
   *
   * <p>When unset, the value tuned by the Bigtable client library is used.
   *
   * <p>Channel pools resize at most once per minute and by at most two channels at a time. Dynamic
   * resizing therefore cannot react to a burst shorter than a minute, so this setting and {@link
   * #initialChannelCount()} are what actually determine burst headroom.
   *
   * <p>Note that setting this to the same value as {@link #maxChannelCount()} turns the channel
   * pool into a statically sized one, which <em>disables all dynamic resizing</em>.
   *
   * <p>Note also that this must not exceed {@link #maxRpcsPerChannel()}, whose inherited value is
   * typically much smaller than the maximum channel count; raising this above that threshold
   * requires raising the threshold as well.
   *
   * <p>When this exceeds an inherited {@link #maxChannelCount()}, the latter is raised to match.
   */
  OptionalInt minChannelCount();

  /**
   * Maximum number of gRPC channels. Refer to Google docs for details.
   *
   * <p>When unset, the value tuned by the Bigtable client library is used.
   *
   * <p>Note that setting this to the same value as {@link #minChannelCount()} turns the channel
   * pool into a statically sized one, which <em>disables all dynamic resizing</em>.
   *
   * <p>When this is below an inherited {@link #minChannelCount()}, the latter is lowered to match.
   */
  OptionalInt maxChannelCount();

  /**
   * Minimum number of RPCs per channel, the threshold below which channels are removed from the
   * pool. Refer to Google docs for details.
   *
   * <p>When unset, the value tuned by the Bigtable client library is used.
   *
   * <p>Note that with a value of {@code 0}, the pool only ever scales down after an interval in
   * which no RPCs were outstanding at all.
   *
   * <p>When this exceeds an inherited {@link #maxRpcsPerChannel()}, the latter is raised to match.
   */
  OptionalInt minRpcsPerChannel();

  /**
   * Maximum number of RPCs per channel, the threshold above which channels are added to the pool.
   * Refer to Google docs for details.
   *
   * <p>When unset, the value tuned by the Bigtable client library is used.
   *
   * <p>Note that gRPC starts queuing requests locally beyond 100 concurrent RPCs per channel, so
   * this should be left well below that. Setting this to {@link Integer#MAX_VALUE} while {@link
   * #minRpcsPerChannel()} is {@code 0} turns the channel pool into a statically sized one, which
   * <em>disables all dynamic resizing</em>.
   *
   * <p>When this is below an inherited {@link #minRpcsPerChannel()}, the latter is lowered to
   * match.
   */
  OptionalInt maxRpcsPerChannel();

  /** Enables telemetry with OpenCensus. */
  default boolean enableTelemetry() {
    return true;
  }
}

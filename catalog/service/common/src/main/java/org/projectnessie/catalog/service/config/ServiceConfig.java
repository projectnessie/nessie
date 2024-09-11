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
package org.projectnessie.catalog.service.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Optional;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableServiceConfig.class)
@JsonDeserialize(as = ImmutableServiceConfig.class)
public interface ServiceConfig {
  /**
   * Nessie tries to verify the connectivity to the object stores configured for each warehouse and
   * exposes this information as a readiness check. It is recommended to leave this setting enabled.
   */
  @ConfigItem(section = "error-handling")
  @WithName("object-stores.health-check.enabled")
  @WithDefault("true")
  boolean objectStoresHealthCheck();

  /**
   * Advanced property. The time interval after which a request is retried when storage I/O responds
   * with some "retry later" response.
   */
  @ConfigPropertyName("throttled-retry-after")
  @ConfigItem(section = "error-handling")
  @WithName("error-handling.throttled-retry-after")
  @WithDefault("PT10S")
  Optional<Duration> retryAfterThrottled();

  default Duration effectiveRetryAfterThrottled() {
    return retryAfterThrottled().orElse(Duration.ofSeconds(10));
  }
}

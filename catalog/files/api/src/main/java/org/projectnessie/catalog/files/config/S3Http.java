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
import java.util.OptionalInt;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableS3Http.class)
@JsonDeserialize(as = ImmutableS3Http.class)
public interface S3Http {

  /** Override the default maximum number of pooled connections. */
  @ConfigItem(section = "transport")
  OptionalInt maxHttpConnections();

  /** Override the default connection read timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> readTimeout();

  /** Override the default TCP connect timeout. */
  @ConfigItem(section = "transport")
  Optional<Duration> connectTimeout();

  /**
   * Override default connection acquisition timeout. This is the time a request will wait for a
   * connection from the pool.
   */
  @ConfigItem(section = "transport")
  Optional<Duration> connectionAcquisitionTimeout();

  /** Override default max idle time of a pooled connection. */
  @ConfigItem(section = "transport")
  Optional<Duration> connectionMaxIdleTime();

  /** Override default time-time of a pooled connection. */
  @ConfigItem(section = "transport")
  Optional<Duration> connectionTimeToLive();

  /** Override default behavior whether to expect an HTTP/100-Continue. */
  @ConfigItem(section = "transport")
  Optional<Boolean> expectContinueEnabled();
}

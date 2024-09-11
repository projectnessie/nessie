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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;

/**
 * Configuration for ADLS Gen2 object stores.
 *
 * <p>Default settings to be applied to all "file systems" (think: buckets) can be set in the {@code
 * default-options} group. Specific settings for each file system can be specified via the {@code
 * file-systems} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the ADLS client
 * supplied by Microsoft. See <a
 * href="https://learn.microsoft.com/en-us/azure/developer/java/sdk/">Azure SDK for Java
 * documentation</a>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableAdlsConfig.class)
@JsonDeserialize(as = ImmutableAdlsConfig.class)
public interface AdlsConfig {

  /**
   * Override the default maximum number of HTTP connections that Nessie can use against all ADLS
   * Gen2 object stores.
   */
  @ConfigItem(section = "transport")
  OptionalInt maxHttpConnections();

  /**
   * Override the default TCP connect timeout for HTTP connections against ADLS Gen2 object stores.
   */
  @ConfigItem(section = "transport")
  Optional<Duration> connectTimeout();

  /** Override the default idle timeout for HTTP connections. */
  @ConfigItem(section = "transport")
  Optional<Duration> connectionIdleTimeout();

  /** Override the default write timeout for HTTP connections. */
  @ConfigItem(section = "transport")
  Optional<Duration> writeTimeout();

  /** Override the default read timeout for HTTP connections. */
  @ConfigItem(section = "transport")
  Optional<Duration> readTimeout();

  /** For configuration options, see {@code com.azure.core.util.Configuration}. */
  Map<String, String> configuration();

  static ImmutableAdlsConfig.Builder builder() {
    return ImmutableAdlsConfig.builder();
  }
}

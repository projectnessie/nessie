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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;

@Value.Immutable
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

  /** For configuration options, see {@link com.azure.core.util.Configuration}. */
  Map<String, String> configurationOptions();

  static Builder builder() {
    return ImmutableAdlsConfig.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder maxHttpConnections(int maxHttpConnections);

    @CanIgnoreReturnValue
    Builder connectTimeout(Duration connectTimeout);

    @CanIgnoreReturnValue
    Builder connectionIdleTimeout(Duration connectionIdleTimeout);

    @CanIgnoreReturnValue
    Builder writeTimeout(Duration writeTimeout);

    @CanIgnoreReturnValue
    Builder readTimeout(Duration readTimeout);

    @CanIgnoreReturnValue
    Builder putConfigurationOptions(String key, String value);

    @CanIgnoreReturnValue
    Builder putConfigurationOptions(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder configurationOptions(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllConfigurationOptions(Map<String, ? extends String> entries);

    AdlsConfig build();
  }
}

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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.projectnessie.catalog.files.adls.AdlsConfig;
import org.projectnessie.catalog.files.adls.AdlsOptions;

/**
 * Configuration for ADLS Gen2 object stores.
 *
 * <p>Contains the default settings to be applied to all "file systems" (think: buckets). Specific
 * settings for each file system can be specified via the {@code file-systems} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the ADLS client
 * supplied by Microsoft. See <a
 * href="https://learn.microsoft.com/en-us/azure/developer/java/sdk/">Azure SDK for Java
 * documentation</a>
 */
@ConfigMapping(prefix = "nessie.catalog.service.adls")
public interface CatalogAdlsConfig extends AdlsConfig, AdlsOptions<CatalogAdlsFileSystemOptions> {
  /**
   * Override the default maximum number of HTTP connections that Nessie can use against all ADLS
   * Gen2 object stores.
   */
  @Override
  OptionalInt maxHttpConnections();

  /**
   * Override the default TCP connect timeout for HTTP connections against ADLS Gen2 object stores.
   */
  @Override
  Optional<Duration> connectTimeout();

  /** Override the default idle timeout for HTTP connections. */
  @Override
  Optional<Duration> connectionIdleTimeout();

  /** Override the default write timeout for HTTP connections. */
  @Override
  Optional<Duration> writeTimeout();

  /** Override the default read timeout for HTTP connections. */
  @Override
  Optional<Duration> readTimeout();

  /** Override the default response timeout for HTTP connections. */
  @Override
  Optional<Duration> responseTimeout();

  /** Custom settings for the ADLS Java client. */
  @WithName("configuration")
  @Override
  Map<String, String> configurationOptions();

  @Override
  OptionalLong writeBlockSize();

  @Override
  OptionalInt readBlockSize();

  // file-system options

  @Override
  Optional<String> accountNameRef();

  @Override
  Optional<String> accountKeyRef();

  @Override
  Optional<String> sasTokenRef();

  @Override
  Optional<String> endpoint();

  @Override
  Optional<String> externalEndpoint();

  @Override
  Optional<AdlsRetryStrategy> retryPolicy();

  @Override
  Optional<Integer> maxRetries();

  @Override
  Optional<Duration> tryTimeout();

  @Override
  Optional<Duration> retryDelay();

  @Override
  Optional<Duration> maxRetryDelay();

  @Override
  Map<String, CatalogAdlsFileSystemOptions> fileSystems();
}

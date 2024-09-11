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
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;

/**
 * Configuration for S3 compatible object stores.
 *
 * <p>Default settings to be applied to all buckets can be set in the {@code default-options} group.
 * Specific settings for each bucket can be specified via the {@code buckets} map.
 *
 * <p>All settings are optional. The defaults of these settings are defined by the AWSSDK Java
 * client.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableS3Config.class)
@JsonDeserialize(as = ImmutableS3Config.class)
public interface S3Config {

  Optional<S3Http> http();

  /**
   * Instruct the S3 HTTP client to accept all SSL certificates, if set to {@code true}. Enabling
   * this option is dangerous, it is strongly recommended to leave this option unset or {@code
   * false}.
   */
  @ConfigItem(section = "transport")
  Optional<Boolean> trustAllCertificates();

  /** Optional: configure the {@code trust-store} for custom SSL trust store. */
  @ConfigItem(section = "transport")
  Optional<SecretStore> trustStore();

  /** Optional: configure a {@code key-store} for custom SSL keys. */
  @ConfigItem(section = "transport")
  Optional<SecretStore> keyStore();

  static ImmutableS3Config.Builder builder() {
    return ImmutableS3Config.builder();
  }
}

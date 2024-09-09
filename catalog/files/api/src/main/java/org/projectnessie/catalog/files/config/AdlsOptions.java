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

import io.smallrye.config.ConfigMapping;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

@ConfigMapping(prefix = "nessie.catalog.service.adls")
public interface AdlsOptions {

  /** Override the default read block size used when writing to ADLS. */
  OptionalInt readBlockSize();

  /** Override the default write block size used when writing to ADLS. */
  OptionalLong writeBlockSize();

  /**
   * Default file-system configuration, default/fallback values for all file-systems are taken from
   * this one.
   */
  @ConfigItem(section = "default-options")
  Optional<? extends AdlsFileSystemOptions> defaultOptions();

  /** ADLS file-system specific options, per file system name. */
  @ConfigItem(section = "buckets")
  @ConfigPropertyName("filesystem-name")
  Map<String, ? extends AdlsNamedFileSystemOptions> fileSystems();

  default void validate() {
    boolean hasDefaultEndpoint = defaultOptions().map(o -> o.endpoint().isPresent()).orElse(false);
    if (!hasDefaultEndpoint && !fileSystems().isEmpty()) {
      List<String> missing =
          fileSystems().entrySet().stream()
              .filter(e -> e.getValue().endpoint().isEmpty())
              .map(Map.Entry::getKey)
              .sorted()
              .collect(Collectors.toList());
      if (!missing.isEmpty()) {
        String msg =
            missing.stream()
                .collect(
                    Collectors.joining(
                        "', '",
                        "Mandatory ADLS endpoint is not configured for file system '",
                        "'."));
        throw new IllegalStateException(msg);
      }
    }
  }

  default AdlsFileSystemOptions effectiveOptionsForFileSystem(Optional<String> filesystemName) {
    AdlsFileSystemOptions defaultOptions =
        defaultOptions()
            .map(AdlsFileSystemOptions.class::cast)
            .orElse(AdlsNamedFileSystemOptions.FALLBACK);
    if (filesystemName.isEmpty()) {
      return defaultOptions;
    }

    AdlsFileSystemOptions specific = fileSystems().get(filesystemName.get());

    if (specific == null) {
      return defaultOptions;
    }

    return ImmutableAdlsNamedFileSystemOptions.builder()
        .from(defaultOptions)
        .from(specific)
        .build();
  }
}

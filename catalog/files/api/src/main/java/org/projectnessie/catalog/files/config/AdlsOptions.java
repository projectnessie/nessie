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

import static org.projectnessie.catalog.files.config.OptionsUtil.resolveSpecializedBucket;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigItem;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.storage.uri.StorageUri;

@NessieImmutable
@JsonSerialize(as = ImmutableAdlsOptions.class)
@JsonDeserialize(as = ImmutableAdlsOptions.class)
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
  Optional<AdlsFileSystemOptions> defaultOptions();

  /**
   * Per-bucket configurations. The effective value for a bucket is taken from the per-bucket
   * setting. If no per-bucket setting is present, uses the defaults from the top-level ADLS
   * settings in {@code default-options}.
   */
  @ConfigItem(section = "buckets")
  @ConfigPropertyName("key")
  Map<String, AdlsNamedFileSystemOptions> fileSystems();

  default AdlsOptions validate() {
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
    return this;
  }

  default AdlsNamedFileSystemOptions resolveOptionsForUri(StorageUri uri) {
    Optional<AdlsNamedFileSystemOptions> specific = resolveSpecializedBucket(uri, fileSystems());

    ImmutableAdlsNamedFileSystemOptions.Builder builder =
        ImmutableAdlsNamedFileSystemOptions.builder();
    defaultOptions().ifPresent(builder::from);
    specific.ifPresent(builder::from);
    return builder.build();
  }

  @Value.NonAttribute
  @JsonIgnore
  default AdlsOptions deepClone() {
    ImmutableAdlsOptions.Builder b =
        ImmutableAdlsOptions.builder().from(this).fileSystems(Map.of());
    defaultOptions().ifPresent(v -> b.defaultOptions(v.deepClone()));
    fileSystems().forEach((n, v) -> b.putFileSystem(n, v.deepClone()));
    return b.build();
  }
}

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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableAdlsNamedFileSystemOptions.class)
@JsonDeserialize(as = ImmutableAdlsNamedFileSystemOptions.class)
@SuppressWarnings("immutables:subtype")
public interface AdlsNamedFileSystemOptions extends AdlsFileSystemOptions {

  AdlsFileSystemOptions FALLBACK = ImmutableAdlsNamedFileSystemOptions.builder().build();

  /**
   * The name of the filesystem. If unset, the name of the bucket will be extracted from the
   * configuration option, e.g. if {@code
   * nessie.catalog.service.adls.filesystem1.name=my-filesystem} is set, the name of the filesystem
   * will be {@code my-filesystem}; otherwise, it will be {@code filesystem1}.
   *
   * <p>This should only be defined if the filesystem name contains non-alphanumeric characters,
   * such as dots or dashes.
   */
  Optional<String> name();

  @Value.NonAttribute
  @JsonIgnore
  default AdlsNamedFileSystemOptions deepCopy() {
    return ImmutableAdlsNamedFileSystemOptions.builder()
        .from(AdlsFileSystemOptions.super.deepCopy())
        .name(name())
        .build();
  }
}

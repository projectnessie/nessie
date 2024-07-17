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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@Value.Style(allParameters = false)
@SuppressWarnings("immutables:from") // defaultOptions + fileSystems are not copied
public abstract class AdlsProgrammaticOptions implements AdlsOptions {

  @Override
  public abstract Optional<AdlsFileSystemOptions> defaultOptions();

  @Override
  public abstract Map<String, AdlsNamedFileSystemOptions> fileSystems();

  public static AdlsOptions normalize(AdlsOptions adlsOptions) {
    ImmutableAdlsProgrammaticOptions.Builder builder =
        ImmutableAdlsProgrammaticOptions.builder().from(adlsOptions);
    // not copied by from() because of different type parameters in method return types
    builder.defaultOptions(adlsOptions.defaultOptions());
    builder.fileSystems(adlsOptions.fileSystems());
    return builder.build();
  }

  @Value.Check
  protected AdlsProgrammaticOptions normalizeBuckets() {
    Map<String, AdlsNamedFileSystemOptions> fileSystems = new HashMap<>();
    for (String fileSystemName : fileSystems().keySet()) {
      AdlsNamedFileSystemOptions options = fileSystems().get(fileSystemName);
      if (options.name().isPresent()) {
        fileSystemName = options.name().get();
      } else {
        options =
            ImmutableAdlsNamedFileSystemOptions.builder()
                .from(options)
                .name(fileSystemName)
                .build();
      }
      if (fileSystems.put(fileSystemName, options) != null) {
        throw new IllegalArgumentException(
            "Duplicate ADLS filesystem name '"
                + fileSystemName
                + "', check your ADLS file system configurations");
      }
    }
    if (fileSystems.equals(fileSystems())) {
      return this;
    }
    return ImmutableAdlsProgrammaticOptions.builder()
        .from(this)
        .defaultOptions(defaultOptions())
        .fileSystems(fileSystems)
        .build();
  }
}

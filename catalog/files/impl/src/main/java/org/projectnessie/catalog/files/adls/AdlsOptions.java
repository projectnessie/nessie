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

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.projectnessie.catalog.files.adls.AdlsProgrammaticOptions.AdlsPerFileSystemOptions;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface AdlsOptions<PER_FILE_SYSTEM extends AdlsFileSystemOptions>
    extends AdlsFileSystemOptions {

  /** For configuration options, see {@link com.azure.core.util.Configuration}. */
  Map<String, String> configurationOptions();

  /** Override the default read block size used when writing to ADLS. */
  OptionalInt readBlockSize();

  /** Override the default write block size used when writing to ADLS. */
  OptionalLong writeBlockSize();

  /** ADLS file-system specific options, per file system name. */
  @ConfigPropertyName("filesystem-name")
  Map<String, PER_FILE_SYSTEM> fileSystems();

  default AdlsFileSystemOptions effectiveOptionsForFileSystem(Optional<String> filesystemName) {
    if (filesystemName.isEmpty()) {
      return this;
    }
    AdlsFileSystemOptions perBucket = fileSystems().get(filesystemName.get());
    if (perBucket == null) {
      return this;
    }

    return AdlsPerFileSystemOptions.builder().from(this).from(perBucket).build();
  }
}

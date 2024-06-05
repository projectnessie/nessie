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

import static org.projectnessie.catalog.secrets.SecretAttribute.secretAttribute;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.projectnessie.catalog.files.adls.AdlsProgrammaticOptions.AdlsPerFileSystemOptions;
import org.projectnessie.catalog.secrets.SecretAttribute;
import org.projectnessie.catalog.secrets.SecretType;
import org.projectnessie.catalog.secrets.SecretsProvider;
import org.projectnessie.nessie.docgen.annotations.ConfigDocs.ConfigPropertyName;

public interface AdlsOptions<PER_FILE_SYSTEM extends AdlsFileSystemOptions>
    extends AdlsFileSystemOptions {

  /**
   * For custom ADLS configuration options, consult javadocs for {@code
   * com.azure.core.util.Configuration}.
   */
  Map<String, String> configurationOptions();

  /** Override the default read block size used when writing to ADLS. */
  OptionalInt readBlockSize();

  /** Override the default write block size used when writing to ADLS. */
  OptionalLong writeBlockSize();

  /** ADLS file-system specific options, per file system name. */
  @ConfigPropertyName("filesystem-name")
  Map<String, PER_FILE_SYSTEM> fileSystems();

  default AdlsFileSystemOptions effectiveOptionsForFileSystem(
      Optional<String> filesystemName, SecretsProvider secretsProvider) {
    if (filesystemName.isEmpty()) {
      return resolveSecrets(null, null, secretsProvider);
    }
    String name = filesystemName.get();
    AdlsFileSystemOptions perBucket = fileSystems().get(name);
    if (perBucket == null) {
      return resolveSecrets(name, null, secretsProvider);
    }

    return resolveSecrets(name, perBucket, secretsProvider);
  }

  List<SecretAttribute<AdlsFileSystemOptions, AdlsPerFileSystemOptions.Builder, ?>>
      SECRET_ATTRIBUTES =
          ImmutableList.of(
              secretAttribute(
                  "account",
                  SecretType.BASIC,
                  AdlsFileSystemOptions::account,
                  AdlsPerFileSystemOptions.Builder::account),
              secretAttribute(
                  "sasToken",
                  SecretType.KEY,
                  AdlsFileSystemOptions::sasToken,
                  AdlsPerFileSystemOptions.Builder::sasToken));

  default AdlsFileSystemOptions resolveSecrets(
      String filesystemName, AdlsFileSystemOptions specific, SecretsProvider secretsProvider) {
    AdlsPerFileSystemOptions.Builder builder = AdlsPerFileSystemOptions.builder().from(this);
    if (specific != null) {
      builder.from(specific);
    }

    return secretsProvider
        .applySecrets(
            builder, "object-stores.adls", this, filesystemName, specific, SECRET_ATTRIBUTES)
        .build();
  }
}

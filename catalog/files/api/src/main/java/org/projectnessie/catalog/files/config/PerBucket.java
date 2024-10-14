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

import java.util.Optional;

public interface PerBucket extends BucketOptions {
  /**
   * The human consumable name of the bucket. If unset, the name of the bucket will be extracted
   * from the configuration option name, e.g. if {@code
   * nessie.catalog.service.s3.bucket1.name=my-bucket} is set, the bucket name will be {@code
   * my-bucket}; otherwise, it will be {@code bucket1}.
   *
   * <p>This can be used; if the bucket name contains non-alphanumeric characters, such as dots or
   * dashes.
   */
  Optional<String> name();

  /**
   * The authority part in a storage location URI. This is the bucket name for S3 and GCS, for ADLS
   * this is the storage account name (optionally prefixed with the container/file-system name).
   * Defaults to {@link #name()}.
   *
   * <p>For S3 and GCS this option should mention the name of the bucket.
   *
   * <p>For ADLS: The value of this option is using the {@code container@storageAccount} syntax. It
   * is mentioned as {@code <file_system>@<account_name>} in the <a
   * href="https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction-abfs-uri">Azure
   * Docs</a>. Note that the {@code <file_system>@} part is optional, {@code <account_name>} is the
   * fully qualified name, usually ending in {@code .dfs.core.windows.net}.
   */
  Optional<String> authority();

  /** The path prefix for this storage location. */
  Optional<String> pathPrefix();
}

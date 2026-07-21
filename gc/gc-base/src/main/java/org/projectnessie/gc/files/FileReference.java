/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.files;

import static com.google.common.base.Preconditions.checkArgument;

import org.immutables.value.Value;
import org.projectnessie.storage.uri.StorageUri;

/**
 * References a file using a {@link #base()} URI plus a {@link #path()}, which is usually relative
 * to {@link #base()}.
 *
 * <p>The {@link #path()} is absolute for files that are not located under {@link #base()}, for
 * example Iceberg data files that live outside the location declared in the table's metadata (via
 * {@code write.data.path}, {@code write.metadata.path}, a changed table location or the like).
 */
@Value.Immutable
public interface FileReference {

  /**
   * URI to the file/directory, relative to {@link #base()} if the file is located under {@link
   * #base()}, otherwise the absolute URI of the file.
   */
  @Value.Parameter(order = 1)
  StorageUri path();

  /** Base location as from for example Iceberg's table-metadata. */
  @Value.Parameter(order = 2)
  StorageUri base();

  /** The file's last modification timestamp, if available, or {@code -1L} if not available. */
  @Value.Parameter(order = 3)
  @Value.Auxiliary
  long modificationTimeMillisEpoch();

  /**
   * Absolute path to the file/directory. Virtually equivalent to {@code base().resolve(path())},
   * which yields {@link #path()} itself, if {@link #path()} is absolute.
   */
  @Value.Lazy
  default StorageUri absolutePath() {
    return base().resolve(path());
  }

  @Value.Check
  default void check() {
    checkArgument(base().isAbsolute(), "Base location must be absolute: %s", base());
  }

  static ImmutableFileReference.Builder builder() {
    return ImmutableFileReference.builder();
  }

  static FileReference of(StorageUri path, StorageUri base, long modificationTimeMillisEpoch) {
    return ImmutableFileReference.of(path, base, modificationTimeMillisEpoch);
  }
}

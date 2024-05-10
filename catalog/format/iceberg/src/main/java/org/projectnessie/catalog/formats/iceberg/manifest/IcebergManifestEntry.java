/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.formats.iceberg.manifest;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import javax.annotation.Nullable;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface IcebergManifestEntry {

  static Builder builder() {
    return ImmutableIcebergManifestEntry.builder();
  }

  IcebergManifestEntryStatus status();

  @Nullable
  @jakarta.annotation.Nullable
  Long snapshotId();

  @Nullable
  @jakarta.annotation.Nullable
  Long sequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  Long fileSequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  IcebergDataFile dataFile();

  @SuppressWarnings("unused")
  interface Builder {

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder from(IcebergManifestEntry entry);

    @CanIgnoreReturnValue
    Builder status(IcebergManifestEntryStatus status);

    @CanIgnoreReturnValue
    Builder snapshotId(@Nullable Long snapshotId);

    @CanIgnoreReturnValue
    Builder sequenceNumber(@Nullable Long sequenceNumber);

    @CanIgnoreReturnValue
    Builder fileSequenceNumber(@Nullable Long fileSequenceNumber);

    @CanIgnoreReturnValue
    Builder dataFile(@Nullable IcebergDataFile dataFile);

    IcebergManifestEntry build();
  }
}

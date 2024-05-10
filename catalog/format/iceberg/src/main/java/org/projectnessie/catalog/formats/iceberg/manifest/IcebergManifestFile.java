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
import java.util.List;
import javax.annotation.Nullable;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionFieldSummary;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface IcebergManifestFile {
  static Builder builder() {
    return ImmutableIcebergManifestFile.builder();
  }

  String manifestPath();

  long manifestLength();

  int partitionSpecId();

  @Nullable
  @jakarta.annotation.Nullable
  Long addedSnapshotId();

  @Nullable
  @jakarta.annotation.Nullable
  Integer addedFilesCount();

  @Nullable
  @jakarta.annotation.Nullable
  Integer existingFilesCount();

  @Nullable
  @jakarta.annotation.Nullable
  Integer deletedFilesCount();

  @Nullable
  @jakarta.annotation.Nullable
  Long addedRowsCount();

  @Nullable
  @jakarta.annotation.Nullable
  Long existingRowsCount();

  @Nullable
  @jakarta.annotation.Nullable
  Long deletedRowsCount();

  @Nullable
  @jakarta.annotation.Nullable
  Long sequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  Long minSequenceNumber();

  @Nullable
  @jakarta.annotation.Nullable
  IcebergManifestContent content();

  @Nullable
  @jakarta.annotation.Nullable
  byte[] keyMetadata();

  List<IcebergPartitionFieldSummary> partitions();

  @SuppressWarnings("unused")
  interface Builder {
    IcebergManifestFile build();

    @CanIgnoreReturnValue
    Builder from(IcebergManifestFile icebergManifestFile);

    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder manifestPath(String manifestPath);

    @CanIgnoreReturnValue
    Builder manifestLength(long manifestLength);

    @CanIgnoreReturnValue
    Builder partitionSpecId(int partitionSpecId);

    @CanIgnoreReturnValue
    Builder addedSnapshotId(@Nullable Long addedSnapshotId);

    @CanIgnoreReturnValue
    Builder addedFilesCount(@Nullable Integer addedFilesCount);

    @CanIgnoreReturnValue
    Builder existingFilesCount(@Nullable Integer existingFilesCount);

    @CanIgnoreReturnValue
    Builder deletedFilesCount(@Nullable Integer deletedFilesCount);

    @CanIgnoreReturnValue
    Builder addedRowsCount(@Nullable Long addedRowsCount);

    @CanIgnoreReturnValue
    Builder existingRowsCount(@Nullable Long existingRowsCount);

    @CanIgnoreReturnValue
    Builder deletedRowsCount(@Nullable Long existingRowsCount);

    @CanIgnoreReturnValue
    Builder sequenceNumber(@Nullable Long sequenceNumber);

    @CanIgnoreReturnValue
    Builder minSequenceNumber(@Nullable Long minSequenceNumber);

    @CanIgnoreReturnValue
    Builder content(@Nullable IcebergManifestContent content);

    @CanIgnoreReturnValue
    Builder keyMetadata(@Nullable byte[] keyMetadata);

    @CanIgnoreReturnValue
    Builder addPartition(IcebergPartitionFieldSummary element);

    @CanIgnoreReturnValue
    Builder addPartitions(IcebergPartitionFieldSummary... elements);

    @CanIgnoreReturnValue
    Builder partitions(Iterable<? extends IcebergPartitionFieldSummary> elements);

    @CanIgnoreReturnValue
    Builder addAllPartitions(Iterable<? extends IcebergPartitionFieldSummary> elements);
  }
}

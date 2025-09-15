/*
 * Copyright (C) 2025 Dremio
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
package org.projectnessie.catalog.formats.iceberg;

import java.util.List;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

public class GenManifestFile implements ManifestFile {

  private final String path;
  private final long length;
  private final int partitionSpecId;
  private final ManifestContent content;
  private final long sequenceNumber;
  private final long minSequenceNumber;
  private final Long snapshotId;
  private final Integer addedFilesCount;
  private final Long addedRowsCount;
  private final Integer existingFilesCount;
  private final Long existingRowsCount;
  private final Integer deletedFilesCount;
  private final Long deletedRowsCount;
  private final List<PartitionFieldSummary> partitions;

  public GenManifestFile(
      String path,
      long length,
      int partitionSpecId,
      ManifestContent content,
      long sequenceNumber,
      long minSequenceNumber,
      Long snapshotId,
      Integer addedFilesCount,
      Long addedRowsCount,
      Integer existingFilesCount,
      Long existingRowsCount,
      Integer deletedFilesCount,
      Long deletedRowsCount,
      List<PartitionFieldSummary> partitions) {
    this.path = path;
    this.length = length;
    this.partitionSpecId = partitionSpecId;
    this.content = content;
    this.sequenceNumber = sequenceNumber;
    this.minSequenceNumber = minSequenceNumber;
    this.snapshotId = snapshotId;
    this.addedFilesCount = addedFilesCount;
    this.addedRowsCount = addedRowsCount;
    this.existingFilesCount = existingFilesCount;
    this.existingRowsCount = existingRowsCount;
    this.deletedFilesCount = deletedFilesCount;
    this.deletedRowsCount = deletedRowsCount;
    this.partitions = partitions;
  }

  @Override
  public String path() {
    return path;
  }

  @Override
  public long length() {
    return length;
  }

  @Override
  public int partitionSpecId() {
    return partitionSpecId;
  }

  @Override
  public ManifestContent content() {
    return content;
  }

  @Override
  public long sequenceNumber() {
    return sequenceNumber;
  }

  @Override
  public long minSequenceNumber() {
    return minSequenceNumber;
  }

  @Override
  public Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Integer addedFilesCount() {
    return addedFilesCount;
  }

  @Override
  public Long addedRowsCount() {
    return addedRowsCount;
  }

  @Override
  public Integer existingFilesCount() {
    return existingFilesCount;
  }

  @Override
  public Long existingRowsCount() {
    return existingRowsCount;
  }

  @Override
  public Integer deletedFilesCount() {
    return deletedFilesCount;
  }

  @Override
  public Long deletedRowsCount() {
    return deletedRowsCount;
  }

  @Override
  public List<PartitionFieldSummary> partitions() {
    return partitions;
  }

  @Override
  public ManifestFile copy() {
    throw new UnsupportedOperationException();
  }
}

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
package org.projectnessie.catalog.formats.iceberg.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergStatisticsFile.class)
@JsonDeserialize(as = ImmutableIcebergStatisticsFile.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergStatisticsFile {

  static Builder builder() {
    return ImmutableIcebergStatisticsFile.builder();
  }

  static IcebergStatisticsFile statisticsFile(
      long snapshotId,
      String statisticsPath,
      long fileSizeInBytes,
      long fileFooterSizeInBytes,
      List<IcebergBlobMetadata> blobMetadata) {
    return ImmutableIcebergStatisticsFile.of(
        snapshotId, statisticsPath, fileSizeInBytes, fileFooterSizeInBytes, blobMetadata);
  }

  long snapshotId();

  String statisticsPath();

  long fileSizeInBytes();

  long fileFooterSizeInBytes();

  List<IcebergBlobMetadata> blobMetadata();

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder clear();

    @CanIgnoreReturnValue
    Builder snapshotId(long snapshotId);

    @CanIgnoreReturnValue
    Builder statisticsPath(String statisticsPath);

    @CanIgnoreReturnValue
    Builder fileSizeInBytes(long fileSizeInBytes);

    @CanIgnoreReturnValue
    Builder fileFooterSizeInBytes(long fileFooterSizeInBytes);

    IcebergStatisticsFile build();
  }
}

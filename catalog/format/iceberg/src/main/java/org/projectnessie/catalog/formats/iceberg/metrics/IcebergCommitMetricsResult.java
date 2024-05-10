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
package org.projectnessie.catalog.formats.iceberg.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergCommitMetricsResult.class)
@JsonDeserialize(as = ImmutableIcebergCommitMetricsResult.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergCommitMetricsResult {

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergTimerResult totalDuration();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult attempts();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedDataFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedDataFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalDataFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedEqualityDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedPositionalDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedPositionalDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedEqualityDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedRecords();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedRecords();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalRecords();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedFilesSizeBytes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedFilesSizeBytes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalFilesSizeBytes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedPositionalDeletes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedPositionalDeletes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalPositionalDeletes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult addedEqualityDeletes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult removedEqualityDeletes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalEqualityDeletes();

  static Builder builder() {
    return ImmutableIcebergCommitMetricsResult.builder();
  }

  interface Builder {
    IcebergCommitMetricsResult build();
  }
}

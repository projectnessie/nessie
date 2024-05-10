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
@JsonSerialize(as = ImmutableIcebergScanMetricsResult.class)
@JsonDeserialize(as = ImmutableIcebergScanMetricsResult.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergScanMetricsResult {
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergTimerResult totalPlanningDuration();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult resultDataFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult resultDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalDataManifests();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalDeleteManifests();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult scannedDataManifests();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult skippedDataManifests();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalFileSizeInBytes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult totalDeleteFileSizeInBytes();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult skippedDataFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult skippedDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult scannedDeleteManifests();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult skippedDeleteManifests();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult indexedDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult equalityDeleteFiles();

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  IcebergCounterResult positionalDeleteFiles();

  static Builder builder() {
    return ImmutableIcebergScanMetricsResult.builder();
  }

  interface Builder {
    IcebergScanMetricsResult build();
  }
}

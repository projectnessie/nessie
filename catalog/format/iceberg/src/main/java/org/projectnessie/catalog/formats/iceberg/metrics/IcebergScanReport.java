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

import static org.projectnessie.catalog.formats.iceberg.metrics.IcebergMetricsReport.SCAN_REPORT_NAME;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonTypeName(SCAN_REPORT_NAME)
@JsonSerialize(as = ImmutableIcebergScanReport.class)
@JsonDeserialize(as = ImmutableIcebergScanReport.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergScanReport extends IcebergMetricsReport {
  String tableName();

  long snapshotId();

  // TODO add a specific sub type for Nessie Catalog?
  JsonNode filter();

  int schemaId();

  List<Integer> projectedFieldIds();

  List<String> projectedFieldNames();

  IcebergScanMetricsResult metrics();

  Map<String, String> metadata();

  static Builder builder() {
    return ImmutableIcebergScanReport.builder();
  }

  interface Builder {
    IcebergScanReport build();
  }
}

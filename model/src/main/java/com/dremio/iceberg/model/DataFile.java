/*
 * Copyright (C) 2020 Dremio
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
package com.dremio.iceberg.model;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

public class DataFile {
  private final String path;
  private final String fileFormat;
  private final long recordCount;
  private final long fileSyzeInBytes;
  private final Integer ordinal;
  private final List<Integer> sortColumns;
  private final Map<Integer, Long> columnSizes;
  private final Map<Integer, Long> valueCounts;
  private final Map<Integer, Long> nullValueCounts;

  @JsonCreator
  public DataFile(
    @JsonProperty("path") String path,
    @JsonProperty("fileFormat") String fileFormat,
    @JsonProperty("recordCount") long recordCount,
    @JsonProperty("fileSyzeInBytes") long fileSyzeInBytes,
    @JsonProperty("ordinal") Integer ordinal,
    @JsonProperty("sortColumns") List<Integer> sortColumns,
    @JsonProperty("columnSizes") Map<Integer, Long> columnSizes,
    @JsonProperty("valueCounts") Map<Integer, Long> valueCounts,
    @JsonProperty("nullValueCounts") Map<Integer, Long> nullValueCounts
  )                                                                     {
    this.path = path;
    this.fileFormat = fileFormat;
    this.recordCount = recordCount;
    this.fileSyzeInBytes = fileSyzeInBytes;
    this.ordinal = ordinal;
    this.sortColumns = sortColumns;
    this.columnSizes = columnSizes;
    this.valueCounts = valueCounts;
    this.nullValueCounts = nullValueCounts;
  }

  public String getPath() {
    return path;
  }

  public String getFileFormat() {
    return fileFormat;
  }

  public long getRecordCount() {
    return recordCount;
  }

  public long getFileSyzeInBytes() {
    return fileSyzeInBytes;
  }

  public Integer getOrdinal() {
    return ordinal;
  }

  public List<Integer> getSortColumns() {
    return sortColumns;
  }

  public Map<Integer, Long> getColumnSizes() {
    return columnSizes;
  }

  public Map<Integer, Long> getValueCounts() {
    return valueCounts;
  }

  public Map<Integer, Long> getNullValueCounts() {
    return nullValueCounts;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("path", path)
      .add("fileFormat", fileFormat)
      .add("recordCount", recordCount)
      .add("fileSyzeInBytes", fileSyzeInBytes)
      .add("ordinal", ordinal)
      .add("sortColumns", sortColumns)
      .add("columnSizes", columnSizes)
      .add("valueCounts", valueCounts)
      .add("nullValueCounts", nullValueCounts)
      .toString();
  }
}

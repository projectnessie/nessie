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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * API representation of a data file in iceberg.
 */
public final class DataFile {

  private final String path;
  private final String fileFormat;
  private final long recordCount;
  private final long fileSizeInBytes;
  private final Integer ordinal;
  private final List<Integer> sortColumns;
  private final Map<Integer, Long> columnSizes;
  private final Map<Integer, Long> valueCounts;
  private final Map<Integer, Long> nullValueCounts;

  public DataFile() {
    this(null, null, 0L, 0L, null, Lists.newArrayList(), Maps.newHashMap(), Maps.newHashMap(),
         Maps.newHashMap());
  }

  public DataFile(String path,
                  String fileFormat,
                  long recordCount,
                  long fileSyzeInBytes,
                  Integer ordinal,
                  List<Integer> sortColumns,
                  Map<Integer, Long> columnSizes,
                  Map<Integer, Long> valueCounts,
                  Map<Integer, Long> nullValueCounts
  ) {
    this.path = path;
    this.fileFormat = fileFormat;
    this.recordCount = recordCount;
    this.fileSizeInBytes = fileSyzeInBytes;
    this.ordinal = ordinal;
    this.sortColumns = sortColumns == null ? Lists.newArrayList() : sortColumns;
    this.columnSizes = columnSizes == null ? Maps.newHashMap() : columnSizes;
    this.valueCounts = valueCounts == null ? Maps.newHashMap() : valueCounts;
    this.nullValueCounts = nullValueCounts == null ? Maps.newHashMap() : nullValueCounts;
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

  public long getFileSizeInBytes() {
    return fileSizeInBytes;
  }

  public Integer getOrdinal() {
    return ordinal;
  }

  public List<Integer> getSortColumns() {
    return sortColumns;
  }

  public Map<Integer, Long> getColumnSizes() {
    return ImmutableMap.copyOf(columnSizes);
  }

  public Map<Integer, Long> getValueCounts() {
    return ImmutableMap.copyOf(valueCounts);
  }

  public Map<Integer, Long> getNullValueCounts() {
    return ImmutableMap.copyOf(nullValueCounts);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", DataFile.class.getSimpleName() + "[", "]")
      .add("path='" + path + "'")
      .add("fileFormat='" + fileFormat + "'")
      .add("recordCount=" + recordCount)
      .add("fileSyzeInBytes=" + fileSizeInBytes)
      .add("ordinal=" + ordinal)
      .add("sortColumns=" + sortColumns)
      .add("columnSizes=" + columnSizes)
      .add("valueCounts=" + valueCounts)
      .add("nullValueCounts=" + nullValueCounts)
      .toString();
  }
}

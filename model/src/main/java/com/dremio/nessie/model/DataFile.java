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

package com.dremio.nessie.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * API representation of a data file in iceberg.
 */
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableDataFile.class)
@JsonDeserialize(as = ImmutableDataFile.class)
public abstract class DataFile {

  public abstract String getPath();

  public abstract String getFileFormat();

  public abstract long getRecordCount();

  public abstract long getFileSizeInBytes();

  @Nullable
  public abstract Integer getOrdinal();

  @Nullable
  public abstract List<Integer> getSortColumns();

  @Nullable
  public abstract Map<Integer, Long> getColumnSizes();

  @Nullable
  public abstract Map<Integer, Long> getValueCounts();

  @Nullable
  public abstract Map<Integer, Long> getNullValueCounts();

}

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
 * represents an iceberg snapshot in data model.
 */
@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableSnapshot.class)
@JsonDeserialize(as = ImmutableSnapshot.class)
public abstract class Snapshot {

  public abstract long getSnapshotId();

  @Nullable
  public abstract Long getParentId();

  public abstract long getTimestampMillis();

  public abstract String getOperation();

  public abstract Map<String, String> getSummary();

  public abstract List<DataFile> getAddedFiles();

  public abstract List<DataFile> getDeletedFiles();

  public abstract String getManifestLocation();

}

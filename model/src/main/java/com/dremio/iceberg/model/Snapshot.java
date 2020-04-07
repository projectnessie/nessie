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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * represents an iceberg snapshot in data model.
 */
public final class Snapshot {

  private final long snapshotId;
  private final Long parentId;
  private final long timestampMillis;
  private final String operation;
  private final Map<String, String> summary;
  private final List<DataFile> addedFiles;
  private final List<DataFile> deletedFiles;
  private final String manifestLocation;

  public Snapshot() {
    this(0L,
         null,
         0L,
         null,
         Maps.newHashMap(),
         Lists.newArrayList(),
         Lists.newArrayList(),
         null
    );
  }

  public Snapshot(long snapshotId,
                  Long parentId,
                  long timestampMillis,
                  String operation,
                  Map<String, String> summary,
                  List<DataFile> addedFiles,
                  List<DataFile> deletedFiles,
                  String manifestLocation) {

    this.snapshotId = snapshotId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.operation = operation;
    this.summary = summary;
    this.addedFiles = addedFiles;
    this.deletedFiles = deletedFiles;
    this.manifestLocation = manifestLocation;
  }

  public long getSnapshotId() {
    return snapshotId;
  }

  public Long getParentId() {
    return parentId;
  }

  public long getTimestampMillis() {
    return timestampMillis;
  }

  public String getOperation() {
    return operation;
  }

  public Map<String, String> getSummary() {
    return ImmutableMap.copyOf(summary);
  }

  public List<DataFile> getAddedFiles() {
    return ImmutableList.copyOf(addedFiles);
  }

  public List<DataFile> getDeletedFiles() {
    return ImmutableList.copyOf(deletedFiles);
  }

  public String getManifestLocation() {
    return manifestLocation;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", Snapshot.class.getSimpleName() + "[", "]")
      .add("snapshotId=" + snapshotId)
      .add("parentId=" + parentId)
      .add("timestampMillis=" + timestampMillis)
      .add("operation='" + operation + "'")
      .add("summary=" + summary)
      .add("addedFiles=" + addedFiles)
      .add("deletedFiles=" + deletedFiles)
      .add("manifestLocation='" + manifestLocation + "'")
      .toString();
  }
}

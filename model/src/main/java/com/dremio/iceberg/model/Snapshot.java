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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

public class Snapshot {

  private final long snapshotId;
  private final Long parentId;
  private final long timestampMillis;
  private final String operation;
  private final Map<String, String> summary;
  private final Iterable<DataFile> addedFiles;
  private final Iterable<DataFile> deletedFiles;
  private final String manifestLocation;

  @JsonCreator
  public Snapshot(@JsonProperty("snapshotId") long snapshotId,
                  @JsonProperty("parentId") Long parentId,
                  @JsonProperty("timestampMillis") long timestampMillis,
                  @JsonProperty("operation") String operation,
                  @JsonProperty("summary") Map<String, String> summary,
                  @JsonProperty("addedFiles") Iterable<DataFile> addedFiles,
                  @JsonProperty("deletedFiles") Iterable<DataFile> deletedFiles,
                  @JsonProperty("manifestLocation") String manifestLocation) {

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
    return summary;
  }

  public Iterable<DataFile> getAddedFiles() {
    return addedFiles;
  }

  public Iterable<DataFile> getDeletedFiles() {
    return deletedFiles;
  }

  public String getManifestLocation() {
    return manifestLocation;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("snapshotId", snapshotId)
      .add("parentId", parentId)
      .add("timestampMillis", timestampMillis)
      .add("operation", operation)
      .add("summary", summary)
      .add("addedFiles", addedFiles)
      .add("deletedFiles", deletedFiles)
      .add("manifestLocation", manifestLocation)
      .toString();
  }
}

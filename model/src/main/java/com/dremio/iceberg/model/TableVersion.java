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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class TableVersion {
  private final String uuid;
  private final String metadataLocation;
  private final Long version;
  private final long createTime;
  private final Long endTime;
  private final Long snapshotId;

  @JsonCreator
  public TableVersion(@JsonProperty("uuid") String uuid,
                      @JsonProperty("metadataLocation") String metadataLocation,
                      @JsonProperty("version") Long version,
                      @JsonProperty("createTime") long createTime,
                      @JsonProperty("endTime") Long endTime,
                      @JsonProperty("snapshotId") Long snapshotId) {
    this.uuid = uuid;
    this.metadataLocation = metadataLocation;
    this.version = version;
    this.createTime = createTime;
    this.endTime = endTime;
    this.snapshotId = snapshotId;
  }

  public String getUuid() {
    return uuid;
  }

  public String getMetadataLocation() {
    return metadataLocation;
  }

  public Long getVersion() {
    return version;
  }

  public long getCreateTime() {
    return createTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableVersion that = (TableVersion) o;
    return createTime == that.createTime &&
      Objects.equal(uuid, that.uuid) &&
      Objects.equal(metadataLocation, that.metadataLocation) &&
      Objects.equal(version, that.version) &&
      Objects.equal(endTime, that.endTime) &&
      Objects.equal(snapshotId, that.snapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(uuid, metadataLocation, version, createTime, endTime, snapshotId);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("uuid", uuid)
      .add("metadataLocation", metadataLocation)
      .add("version", version)
      .add("createTime", createTime)
      .add("endTime", endTime)
      .add("snapshotId", snapshotId)
      .toString();
  }
}

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

import java.util.Objects;
import java.util.OptionalLong;
import java.util.StringJoiner;

/**
 * minimum set of information to identify an Iceberg table in time.
 */
public final class TableVersion {

  private final String uuid;
  private final String metadataLocation;
  private final long createTime;
  private final Long endTime;
  private final Long snapshotId;

  public TableVersion() {
    this(null, null, 0L, null, null);
  }

  public TableVersion(String uuid,
                      String metadataLocation,
                      long createTime,
                      Long endTime,
                      Long snapshotId) {
    this.uuid = uuid;
    this.metadataLocation = metadataLocation;
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

  public long getCreateTime() {
    return createTime;
  }

  public OptionalLong getEndTime() {
    return endTime == null ? OptionalLong.empty() : OptionalLong.of(endTime);
  }

  public OptionalLong getSnapshotId() {
    return snapshotId == null ? OptionalLong.empty() : OptionalLong.of(snapshotId);
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
    return Objects.equals(metadataLocation, that.metadataLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(metadataLocation);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", TableVersion.class.getSimpleName() + "[", "]")
      .add("uuid='" + uuid + "'")
      .add("metadataLocation='" + metadataLocation + "'")
      .add("createTime=" + createTime)
      .add("endTime=" + endTime)
      .add("snapshotId=" + snapshotId)
      .toString();
  }
}

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
package org.apache.iceberg.view;

import com.google.common.base.MoreObjects;

public class BaseVersion implements Version {
  private final int versionId;
  private final Integer parentId;
  private final long timestampMillis;
  private final VersionSummary summary;
  private final ViewDefinition viewDefinition;

  public BaseVersion(
      int versionId,
      Integer parentId,
      long timestampMillis,
      VersionSummary summary,
      ViewDefinition viewDefinition) {
    this.versionId = versionId;
    this.parentId = parentId;
    this.timestampMillis = timestampMillis;
    this.summary = summary;
    this.viewDefinition = viewDefinition;
  }

  @Override
  public int versionId() {
    return versionId;
  }

  @Override
  public Integer parentId() {
    return parentId;
  }

  @Override
  public long timestampMillis() {
    return timestampMillis;
  }

  @Override
  public VersionSummary summary() {
    return summary;
  }

  @Override
  public ViewDefinition viewDefinition() {
    return viewDefinition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("id", versionId)
        .add("timestamp_ms", timestampMillis)
        .add("summary", summary)
        .add("view_definition", viewDefinition)
        .toString();
  }
}

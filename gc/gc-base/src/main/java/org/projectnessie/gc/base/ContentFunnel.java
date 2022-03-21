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
package org.projectnessie.gc.base;

import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import java.nio.charset.StandardCharsets;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;

/** Bloom filter funnel implementation to hold {@link Content} info. */
enum ContentFunnel implements Funnel<Content> {
  INSTANCE;

  @Override
  public void funnel(Content content, PrimitiveSink into) {
    switch (content.getType()) {
        // For the Iceberg contents before global state removal (Nessie version < 0.26.0)
        // metadataLocation will not be unique.
        // Hence, to handle both the version's compatibility, use snapshotId + metadataLocation as
        // key.
      case ICEBERG_TABLE:
        IcebergTable icebergTable = (IcebergTable) content;
        into.putLong(icebergTable.getSnapshotId())
            .putString(icebergTable.getMetadataLocation(), StandardCharsets.UTF_8);
        break;
      case ICEBERG_VIEW:
        IcebergView icebergView = (IcebergView) content;
        into.putLong(icebergView.getVersionId())
            .putString(icebergView.getMetadataLocation(), StandardCharsets.UTF_8);
        break;
      default:
        throw new RuntimeException("Unsupported type " + content.getType());
    }
  }
}

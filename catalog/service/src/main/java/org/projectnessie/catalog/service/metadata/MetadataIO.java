/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.service.metadata;

import java.io.IOException;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.projectnessie.model.IcebergTable;

public interface MetadataIO {
  TableMetadata store(TableMetadata metadata, int newVersion) throws IOException;

  TableMetadata load(String metadataLocation) throws IOException;

  /**
   * Loads and fixes the {@link TableMetadata}, if necessary, with the IDs that are different from
   * in Nessie's legacy global-state commits.
   */
  default TableMetadata loadAndFixTableMetadata(IcebergTable table) throws IOException {
    String metadataLocation = table.getMetadataLocation();
    TableMetadata loaded = load(metadataLocation);

    // TODO only re-build, if necessary (optimization)

    TableMetadata.Builder builder =
        TableMetadata.buildFrom(loaded)
            .setPreviousFileLocation(null)
            .setCurrentSchema(table.getSchemaId())
            .setDefaultSortOrder(table.getSortOrderId())
            .setDefaultPartitionSpec(table.getSpecId())
            .withMetadataLocation(metadataLocation);
    if (table.getSnapshotId() != -1) {
      builder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);
    }

    return builder.discardChanges().build();
  }

  default int parseVersion(String metadataLocation) {
    if (metadataLocation == null) {
      return -1;
    }

    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      // found filesystem table's metadata
      return -1;
    }

    try {
      return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      return -1;
    }
  }
}

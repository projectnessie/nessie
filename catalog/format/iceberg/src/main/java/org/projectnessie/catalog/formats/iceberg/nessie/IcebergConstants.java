/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.nessie;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

final class IcebergConstants {
  private IcebergConstants() {}

  /**
   * Reserved table property for table format version.
   *
   * <p>Iceberg will default a new table's format version to the latest stable and recommended
   * version. This reserved property keyword allows users to override the Iceberg format version of
   * the table metadata.
   *
   * <p>If this table property exists when creating a table, the table will use the specified format
   * version. If a table updates this property, it will try to upgrade to the specified format
   * version.
   *
   * <p>Note: incomplete or unstable versions cannot be selected using this property.
   */
  static final String FORMAT_VERSION = "format-version";

  /** Reserved table property for table UUID. */
  static final String UUID = "uuid";

  /** Reserved table property for the total number of snapshots. */
  static final String SNAPSHOT_COUNT = "snapshot-count";

  /** Reserved table property for current snapshot summary. */
  static final String CURRENT_SNAPSHOT_SUMMARY = "current-snapshot-summary";

  /** Reserved table property for current snapshot id. */
  static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";

  /** Reserved table property for current snapshot timestamp. */
  static final String CURRENT_SNAPSHOT_TIMESTAMP = "current-snapshot-timestamp-ms";

  /** Reserved table property for the JSON representation of current schema. */
  static final String CURRENT_SCHEMA = "current-schema";

  /** Reserved table property for the JSON representation of current(default) partition spec. */
  static final String DEFAULT_PARTITION_SPEC = "default-partition-spec";

  /** Reserved table property for the JSON representation of current(default) sort order. */
  static final String DEFAULT_SORT_ORDER = "default-sort-order";

  /**
   * Reserved Iceberg table properties list.
   *
   * <p>Reserved table properties are only used to control behaviors when creating or updating a
   * table. The value of these properties are not persisted as a part of the table metadata.
   */
  static final Set<String> RESERVED_PROPERTIES =
      ImmutableSet.of(
          FORMAT_VERSION,
          UUID,
          SNAPSHOT_COUNT,
          CURRENT_SNAPSHOT_ID,
          CURRENT_SNAPSHOT_SUMMARY,
          CURRENT_SNAPSHOT_TIMESTAMP,
          CURRENT_SCHEMA,
          DEFAULT_PARTITION_SPEC,
          DEFAULT_SORT_ORDER);
}

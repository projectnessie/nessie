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

/**
 * Enum serving as a "constants pool" for the string values passed to Nessie access control checks.
 */
public enum CatalogOps {
  // Iceberg metadata updates
  META_ADD_VIEW_VERSION,
  META_SET_CURRENT_VIEW_VERSION,
  META_SET_STATISTICS,
  META_REMOVE_STATISTICS,
  META_SET_PARTITION_STATISTICS,
  META_REMOVE_PARTITION_STATISTICS,
  META_ASSIGN_UUID,
  META_ADD_SCHEMA,
  META_SET_CURRENT_SCHEMA,
  META_ADD_PARTITION_SPEC,
  META_SET_DEFAULT_PARTITION_SPEC,
  META_ADD_SNAPSHOT,
  META_ADD_SORT_ORDER,
  META_SET_DEFAULT_SORT_ORDER,
  META_SET_LOCATION,
  META_SET_PROPERTIES,
  META_REMOVE_PROPERTIES,
  META_REMOVE_LOCATION_PROPERTY,
  META_SET_SNAPSHOT_REF,
  META_REMOVE_SNAPSHOT_REF,
  META_UPGRADE_FORMAT_VERSION,

  // Catalog operations
  CATALOG_CREATE_ENTITY,
  CATALOG_UPDATE_ENTITY,
  CATALOG_DROP_ENTITY,
  CATALOG_RENAME_ENTITY_FROM,
  CATALOG_RENAME_ENTITY_TO,
  CATALOG_REGISTER_ENTITY,
  CATALOG_UPDATE_MULTIPLE,
  CATALOG_S3_SIGN,

  // From Iceberg's snapshot summary
  SNAP_ADD_DATA_FILES,
  SNAP_DELETE_DATA_FILES,
  SNAP_ADD_DELETE_FILES,
  SNAP_ADD_EQUALITY_DELETE_FILES,
  SNAP_ADD_POSITION_DELETE_FILES,
  SNAP_REMOVE_DELETE_FILES,
  SNAP_REMOVE_EQUALITY_DELETE_FILES,
  SNAP_REMOVE_POSITION_DELETE_FILES,
  SNAP_ADDED_RECORDS,
  SNAP_DELETED_RECORDS,
  SNAP_ADDED_POSITION_DELETES,
  SNAP_DELETED_POSITION_DELETES,
  SNAP_ADDED_EQUALITY_DELETES,
  SNAP_DELETED_EQUALITY_DELETES,
  SNAP_REPLACE_PARTITIONS,
  SNAP_OP_APPEND,
  SNAP_OP_REPLACE,
  SNAP_OP_OVERWRITE,
  SNAP_OP_DELETE,
}

/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.model;

public abstract class IcebergContent extends Content {

  /** Constant for {@link GenericMetadata#getVariant()}. */
  public static final String ICEBERG_METADATA_VARIANT = "iceberg";

  // JSON field names - defined by Apache Iceberg spec

  public static final String CURRENT_VERSION_ID = "current-version-id";
  public static final String VERSIONS = "versions";
  public static final String VERSION_ID = "version-id";
  public static final String VIEW_DEFINITION = "view-definition";
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  public static final String CURRENT_SCHEMA_ID = "current-schema-id";
  public static final String DEFAULT_SORT_ORDER_ID = "default-sort-order-id";
  public static final String DEFAULT_SPEC_ID = "default-spec-id";
  public static final String FORMAT_VERSION = "format-version";
  public static final String SNAPSHOTS = "snapshots";
  public static final String SNAPSHOT_ID = "snapshot-id";
  public static final String SCHEMAS = "schemas";
  public static final String SCHEMA_ID = "schema-id";
  public static final String SORT_ORDERS = "sort-orders";
  public static final String ORDER_ID = "order-id";
  public static final String PARTITION_SPECS = "partition-specs";
  public static final String SPEC_ID = "spec-id";
}

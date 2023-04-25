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
package org.projectnessie.events.api;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/** A {@link Content} object that represents an Iceberg table. */
@Value.Immutable
@JsonTypeName("ICEBERG_TABLE")
@JsonSerialize(as = ImmutableIcebergTable.class)
@JsonDeserialize(as = ImmutableIcebergTable.class)
public interface IcebergTable extends Content {

  @Override
  @Value.Default
  default ContentType getType() {
    return ContentType.ICEBERG_TABLE;
  }

  /**
   * Location where Iceberg stored its {@code TableMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentSnapshotId}. */
  long getSnapshotId();

  /** Corresponds to Iceberg's {@code currentSchemaId}. */
  int getSchemaId();

  /** Corresponds to Iceberg's {@code defaultSpecId}. */
  int getSpecId();

  /** Corresponds to Iceberg's {@code defaultSortOrderId}. */
  int getSortOrderId();
}

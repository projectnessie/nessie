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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableIcebergView.class)
@JsonDeserialize(as = ImmutableIcebergView.class)
@JsonTypeName("ICEBERG_VIEW")
public abstract class IcebergView extends IcebergContent {

  @Override
  @Nullable
  @Value.Parameter(order = 1)
  public abstract String getId();

  /**
   * Location where Iceberg stored its {@code ViewMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  @Value.Parameter(order = 2)
  public abstract String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentVersionId}. */
  @Value.Parameter(order = 3)
  public abstract int getVersionId();

  @Value.Parameter(order = 4)
  public abstract int getSchemaId();

  @NotBlank
  @NotNull
  @Value.Parameter(order = 6)
  public abstract String getSqlText();

  @Nullable // TODO this is currently undefined in Iceberg
  @Value.Parameter(order = 5)
  public abstract String getDialect();

  @Override
  public Type getType() {
    return Type.ICEBERG_VIEW;
  }

  @Deprecated
  @Nullable
  @JsonInclude(Include.NON_NULL)
  public abstract GenericMetadata getMetadata();

  public static ImmutableIcebergView.Builder builder() {
    return ImmutableIcebergView.builder();
  }

  public static IcebergView of(
      String metadataLocation, int versionId, int schemaId, String dialect, String sqlText) {
    return ImmutableIcebergView.of(null, metadataLocation, versionId, schemaId, dialect, sqlText);
  }

  public static IcebergView of(
      String id,
      String metadataLocation,
      int versionId,
      int schemaId,
      String dialect,
      String sqlText) {
    return ImmutableIcebergView.of(id, metadataLocation, versionId, schemaId, dialect, sqlText);
  }
}

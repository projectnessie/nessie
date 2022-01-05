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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import org.immutables.value.Value;
import org.projectnessie.model.iceberg.IcebergViewDefinition;
import org.projectnessie.model.iceberg.IcebergViewVersion;

@Value.Immutable
@JsonSerialize(as = ImmutableIcebergView.class)
@JsonDeserialize(as = ImmutableIcebergView.class)
@JsonTypeName("ICEBERG_VIEW")
public abstract class IcebergView extends Content {

  public static final int MIN_SUPPORTED_FORMAT_VERSION = 1;
  public static final int MAX_SUPPORTED_FORMAT_VERSION = 1;

  /**
   * Location where Iceberg stored its {@code ViewMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  public abstract String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentVersionId}. */
  @PositiveOrZero
  public abstract int getVersionId();

  public abstract int getSchemaId();

  @NotBlank
  @NotNull
  public abstract String getSqlText();

  @Nullable // TODO this is currently undefined in Iceberg
  public abstract String getDialect();

  @Override
  public Type getType() {
    return Type.ICEBERG_VIEW;
  }

  @Nullable // for backwards compatibility
  @Min(MIN_SUPPORTED_FORMAT_VERSION)
  @Max(MAX_SUPPORTED_FORMAT_VERSION)
  @JsonInclude(Include.NON_NULL)
  public abstract Integer getFormatVersion();

  @Nullable // for backwards compatibility
  @JsonInclude(Include.NON_EMPTY)
  public abstract Map<String, String> getProperties();

  @Nullable // for backwards compatibility
  @JsonInclude(Include.NON_EMPTY)
  public abstract List<IcebergViewVersion> getVersions();

  @Nullable // for backwards compatibility
  @JsonInclude(Include.NON_NULL)
  public abstract IcebergViewDefinition getViewDefinition();

  public static ImmutableIcebergView.Builder builder() {
    return ImmutableIcebergView.builder();
  }

  public static IcebergView of(
      String metadataLocation, int versionId, int schemaId, String dialect, String sqlText) {
    return builder()
        .metadataLocation(metadataLocation)
        .versionId(versionId)
        .schemaId(schemaId)
        .dialect(dialect)
        .sqlText(sqlText)
        .build();
  }

  public static IcebergView of(
      String id,
      String metadataLocation,
      int versionId,
      int schemaId,
      String dialect,
      String sqlText) {
    return builder()
        .id(id)
        .metadataLocation(metadataLocation)
        .versionId(versionId)
        .schemaId(schemaId)
        .dialect(dialect)
        .sqlText(sqlText)
        .build();
  }
}

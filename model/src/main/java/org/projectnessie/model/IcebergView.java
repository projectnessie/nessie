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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Iterator;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableIcebergView.class)
@JsonDeserialize(as = ImmutableIcebergView.class)
@JsonTypeName("ICEBERG_VIEW")
public abstract class IcebergView extends Content {

  /** Constant for {@link GenericMetadata#getVariant()}. */
  public static final String VIEW_METADATA = "Iceberg";

  /**
   * Location where Iceberg stored its {@code ViewMetadata} file. The location depends on the
   * (implementation of) Iceberg's {@code FileIO} configured for the particular Iceberg table.
   */
  @NotNull
  @NotBlank
  public abstract String getMetadataLocation();

  /** Corresponds to Iceberg's {@code currentVersionId}. */
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

  @Nullable
  @JsonInclude(Include.NON_NULL)
  public abstract GenericMetadata getMetadata();

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

  static final String LOCATION = "location";
  public static final String CURRENT_VERSION_ID = "current-version-id";
  public static final String VERSIONS = "versions";
  public static final String VERSION_ID = "version-id";
  public static final String VIEW_DEFINITION = "view-definition";
  private static final String SCHEMA_ID = "schema-id";

  public static IcebergView of(JsonNode metadata, String metadataLocation, String id) {
    int currentVersionId = metadata.get(CURRENT_VERSION_ID).asInt(-1);
    String sqlText = "";
    String dialect = ""; // TODO !!
    int schemaId = 0;
    for (Iterator<JsonNode> versionIter = metadata.get(VERSIONS).iterator();
        versionIter.hasNext(); ) {
      JsonNode version = versionIter.next();
      if (version.get(VERSION_ID).asInt(-1) == currentVersionId) {
        JsonNode viewDefinition = version.get(VIEW_DEFINITION);
        sqlText = viewDefinition.get("sql").asText();
        JsonNode schema = viewDefinition.get("schema");
        schemaId = schema.get(SCHEMA_ID).asInt(0);
      }
    }

    return builder()
        .id(id)
        .metadataLocation(metadataLocation)
        .versionId(currentVersionId)
        .schemaId(schemaId)
        .dialect(dialect)
        .sqlText(sqlText)
        .metadata(GenericMetadata.of(VIEW_METADATA, metadata))
        .build();
  }
}

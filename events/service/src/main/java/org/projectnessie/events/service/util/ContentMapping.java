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
package org.projectnessie.events.service.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.ImmutableDeltaLakeTable;
import org.projectnessie.events.api.ImmutableGenericContent;
import org.projectnessie.events.api.ImmutableIcebergTable;
import org.projectnessie.events.api.ImmutableIcebergView;
import org.projectnessie.events.api.ImmutableNamespace;
import org.projectnessie.events.api.ImmutableUDF;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.UDF;
import org.projectnessie.model.types.GenericContent;

public final class ContentMapping {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};

  private ContentMapping() {}

  public static Content map(org.projectnessie.model.Content content) {
    org.projectnessie.model.Content.Type type = content.getType();
    if (type == org.projectnessie.model.Content.Type.NAMESPACE) {
      return ImmutableNamespace.builder()
          .id(Objects.requireNonNull(content.getId()))
          .elements(((org.projectnessie.model.Namespace) content).getElements())
          .build();
    } else if (type == org.projectnessie.model.Content.Type.ICEBERG_TABLE) {
      IcebergTable table = (IcebergTable) content;
      return ImmutableIcebergTable.builder()
          .id(Objects.requireNonNull(content.getId()))
          .metadataLocation(table.getMetadataLocation())
          .snapshotId(table.getSnapshotId())
          .schemaId(table.getSchemaId())
          .specId(table.getSpecId())
          .sortOrderId(table.getSortOrderId())
          .build();
    } else if (type == org.projectnessie.model.Content.Type.ICEBERG_VIEW) {
      IcebergView view = (IcebergView) content;
      return ImmutableIcebergView.builder()
          .id(Objects.requireNonNull(content.getId()))
          .metadataLocation(view.getMetadataLocation())
          .versionId(view.getVersionId())
          .schemaId(view.getSchemaId())
          .sqlText(view.getSqlText())
          .dialect(Optional.ofNullable(view.getDialect()))
          .build();
    } else if (type == org.projectnessie.model.Content.Type.DELTA_LAKE_TABLE) {
      DeltaLakeTable table = (DeltaLakeTable) content;
      return ImmutableDeltaLakeTable.builder()
          .id(Objects.requireNonNull(content.getId()))
          .metadataLocationHistory(table.getMetadataLocationHistory())
          .lastCheckpoint(table.getLastCheckpoint())
          .checkpointLocationHistory(table.getCheckpointLocationHistory())
          .build();
    } else if (type == org.projectnessie.model.Content.Type.UDF) {
      UDF udf = (UDF) content;
      return ImmutableUDF.builder()
          .id(Objects.requireNonNull(content.getId()))
          .sqlText(udf.getSqlText())
          .dialect(Optional.ofNullable(udf.getDialect()))
          .build();
    } else if (content instanceof GenericContent) {
      GenericContent genericContent = (GenericContent) content;
      ImmutableGenericContent.Builder builder =
          ImmutableGenericContent.builder()
              .id(Objects.requireNonNull(content.getId()))
              .genericType(genericContent.getType().name());
      if (genericContent.getAttributes() != null) {
        builder.putAllProperties(genericContent.getAttributes());
      }
      return builder.build();
    } else {
      Map<String, ?> map = MAPPER.convertValue(content, MAP_TYPE);
      return ImmutableGenericContent.builder()
          .id((String) map.remove("id"))
          .genericType((String) map.remove("type"))
          .putAllProperties(map)
          .build();
    }
  }

  public static ContentKey map(org.projectnessie.model.ContentKey key) {
    return ContentKey.of(key.getElements());
  }
}

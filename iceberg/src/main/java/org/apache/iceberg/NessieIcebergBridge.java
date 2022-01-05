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
package org.apache.iceberg;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.iceberg.IcebergPartitionSpec;
import org.projectnessie.model.iceberg.IcebergSchema;
import org.projectnessie.model.iceberg.IcebergSnapshot;
import org.projectnessie.model.iceberg.IcebergSortOrder;

/**
 * "Bridge" between Nessie's code underneath {@code org.projectnessie} and a bunch of Iceberg
 * package-private classes/methods.
 */
public abstract class NessieIcebergBridge {

  protected NessieIcebergBridge() {}

  protected static int lastAssignedPartitionId(TableMetadata tableMetadata) {
    return tableMetadata.lastAssignedPartitionId();
  }

  protected static TableMetadata toIcebergInternal(FileIO io, IcebergTable icebergTable) {
    String uuid = icebergTable.getUuid();

    int currentSchemaId = icebergTable.getSchemaId();
    List<Schema> schemas =
        icebergTable.getSchemas().stream()
            .map(NessieIcebergBridge::toIceberg)
            .collect(Collectors.toList());
    Schema currentSchema =
        schemas.stream().filter(s -> s.schemaId() == currentSchemaId).findFirst().orElse(null);

    long currentSnapshotId = icebergTable.getSnapshotId();
    List<Snapshot> snapshots =
        icebergTable.getSnapshots().stream()
            .map(s -> toIceberg(io, s))
            .collect(Collectors.toList());

    List<SortOrder> sortOrders =
        icebergTable.getSortOrders() == null
            ? Collections.emptyList()
            : icebergTable.getSortOrders().stream()
                .map(so -> NessieIcebergBridge.toIceberg(currentSchema, so))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    List<PartitionSpec> specs =
        icebergTable.getSpecs() == null
            ? Collections.emptyList()
            : icebergTable.getSpecs().stream()
                .map(ps -> NessieIcebergBridge.toIceberg(currentSchema, ps))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    int defaultSpecId = icebergTable.getSpecId();
    int defaultSortOrderId = icebergTable.getSortOrderId();

    int formatVersion = icebergTable.getFormatVersion();

    int lastAssignedPartitionId = icebergTable.getLastAssignedPartitionId();
    int lastColumnId = icebergTable.getLastColumnId();
    long lastSequenceNumber = icebergTable.getLastSequenceNumber();
    long lastUpdatedMillis = icebergTable.getLastUpdatedMillis();

    Map<String, String> properties = icebergTable.getProperties();
    String location = icebergTable.getLocation();
    String metadataFileLocation = icebergTable.getMetadataLocation();

    List<HistoryEntry> snapshotLog = Collections.emptyList();
    List<MetadataLogEntry> previousFiles = Collections.emptyList();
    List<MetadataUpdate> changes = Collections.emptyList();

    return new TableMetadata(
        metadataFileLocation,
        formatVersion,
        uuid,
        location,
        lastSequenceNumber,
        lastUpdatedMillis,
        lastColumnId,
        currentSchemaId,
        schemas,
        defaultSpecId,
        specs,
        lastAssignedPartitionId,
        defaultSortOrderId,
        sortOrders,
        properties,
        currentSnapshotId,
        snapshots,
        snapshotLog,
        previousFiles,
        changes);
  }

  private static PartitionSpec toIceberg(Schema schema, IcebergPartitionSpec icebergPartitionSpec) {
    ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
    json.put(SPEC_ID, icebergPartitionSpec.getSpecId());

    ArrayNode fields = json.putArray(FIELDS);
    if (icebergPartitionSpec.getFields() != null) {
      for (IcebergPartitionSpec.PartitionField field : icebergPartitionSpec.getFields()) {
        ObjectNode fieldNode = new ObjectNode(JsonNodeFactory.instance);
        fieldNode.put(NAME, field.getName());
        fieldNode.put(TRANSFORM, field.getTransform());
        fieldNode.put(SOURCE_ID, field.getSourceId());
        if (field.getFieldId() != null) {
          fieldNode.put(FIELD_ID, field.getFieldId().intValue());
        }
        fields.add(fieldNode);
      }
    }

    return PartitionSpecParser.fromJson(schema, json);
  }

  private static SortOrder toIceberg(Schema schema, IcebergSortOrder icebergSortOrder) {
    ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
    json.put(ORDER_ID, icebergSortOrder.getOrderId());

    ArrayNode fields = json.putArray(FIELDS);
    if (icebergSortOrder.getFields() != null) {
      for (IcebergSortOrder.SortField field : icebergSortOrder.getFields()) {
        ObjectNode fieldNode = new ObjectNode(JsonNodeFactory.instance);
        fieldNode.put(TRANSFORM, field.getTransform());
        fieldNode.put(SOURCE_ID, field.getSourceId());
        fieldNode.put(DIRECTION, toIceberg(field.getDirection()));
        fieldNode.put(NULL_ORDER, toIceberg(field.getNullOrder()));
        fields.add(fieldNode);
      }
    }

    return SortOrderParser.fromJson(schema, json);
  }

  private static String toIceberg(IcebergSortOrder.SortField.NullOrder nullOrder) {
    return nullOrder == IcebergSortOrder.SortField.NullOrder.NULLS_FIRST
        ? "nulls-first"
        : "nulls-last";
  }

  private static String toIceberg(IcebergSortOrder.SortField.SortDirection direction) {
    return direction.name().toLowerCase(Locale.ROOT);
  }

  private static Snapshot toIceberg(FileIO io, IcebergSnapshot icebergSnapshot) {
    if (icebergSnapshot == null) {
      return null;
    }
    return new BaseSnapshot(
        io,
        icebergSnapshot.getSequenceNumber(),
        icebergSnapshot.getSnapshotId(),
        icebergSnapshot.getParentId(),
        icebergSnapshot.getTimestampMillis(),
        icebergSnapshot.getOperation(),
        icebergSnapshot.getSummary(),
        icebergSnapshot.getSchemaId(),
        icebergSnapshot.getManifestListLocation());
  }

  protected static Schema toIceberg(IcebergSchema icebergSchema) {
    if (icebergSchema == null) {
      return null;
    }

    ObjectNode json = new ObjectNode(JsonNodeFactory.instance);
    json.put(SCHEMA_ID, icebergSchema.getSchemaId());
    if (icebergSchema.getIdentifierFieldIds() != null) {
      ArrayNode fieldIds = json.putArray(IDENTIFIER_FIELD_IDS);
      for (int identifierFieldId : icebergSchema.getIdentifierFieldIds()) {
        fieldIds.add(identifierFieldId);
      }
    }
    json.putIfAbsent(FIELDS, icebergSchema.getStruct());
    json.put(TYPE, STRUCT);

    return SchemaParser.fromJson(json);
  }

  protected static final String SCHEMA_ID = "schema-id";
  protected static final String IDENTIFIER_FIELD_IDS = "identifier-field-ids";
  protected static final String FIELDS = "fields";
  protected static final String SPEC_ID = "spec-id";
  protected static final String SOURCE_ID = "source-id";
  protected static final String FIELD_ID = "field-id";
  protected static final String TRANSFORM = "transform";
  protected static final String NAME = "name";
  protected static final String ORDER_ID = "order-id";
  protected static final String DIRECTION = "direction";
  protected static final String NULL_ORDER = "null-order";
  protected static final String TYPE = "type";
  protected static final String STRUCT = "struct";
}

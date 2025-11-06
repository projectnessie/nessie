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
package org.projectnessie.catalog.formats.iceberg.nessie;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.time.Instant.now;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace.icebergNamespace;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.INITIAL_SPEC_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.INITIAL_COLUMN_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.INITIAL_SCHEMA_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.INITIAL_SORT_ORDER_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.INITIAL_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.NO_SNAPSHOT_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergViewHistoryEntry.icebergViewHistoryEntry;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.DERIVED_PROPERTIES;
import static org.projectnessie.catalog.formats.iceberg.nessie.IcebergConstants.RESERVED_PROPERTIES;
import static org.projectnessie.catalog.formats.iceberg.nessie.ReuseOrCreate.reuseOrCreate;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.TYPE_LIST;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.TYPE_MAP;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.TYPE_STRUCT;
import static org.projectnessie.catalog.model.id.NessieId.transientNessieId;
import static org.projectnessie.catalog.model.schema.types.NessieType.DEFAULT_TIME_PRECISION;
import static org.projectnessie.catalog.model.snapshot.NessieViewRepresentation.NessieViewSQLRepresentation.nessieViewSQLRepresentation;
import static org.projectnessie.catalog.model.snapshot.TableFormat.ICEBERG;
import static org.projectnessie.catalog.model.statistics.NessiePartitionStatisticsFile.partitionStatisticsFile;
import static org.projectnessie.catalog.model.statistics.NessieStatisticsFile.statisticsFile;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;
import static org.projectnessie.model.Content.Type.NAMESPACE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergBlobMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergHistoryEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotLogEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotRef;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTransform;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewRepresentation;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewRepresentation.IcebergSQLViewRepresentation;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewVersion;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSnapshot;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AssignUUID;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.RemoveProperties;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetProperties;
import org.projectnessie.catalog.formats.iceberg.types.IcebergDecimalType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergFixedType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergListType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergMapType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergStructType;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieFieldTransform;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.schema.NessieSortField;
import org.projectnessie.catalog.model.schema.NessieStruct;
import org.projectnessie.catalog.model.schema.types.NessieDecimalTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieFixedTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieListTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieMapTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieStructTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieTimeTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieTimestampTypeSpec;
import org.projectnessie.catalog.model.schema.types.NessieType;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewRepresentation;
import org.projectnessie.catalog.model.snapshot.NessieViewRepresentation.NessieViewSQLRepresentation;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.model.statistics.NessieIcebergBlobMetadata;
import org.projectnessie.catalog.model.statistics.NessiePartitionStatisticsFile;
import org.projectnessie.catalog.model.statistics.NessieStatisticsFile;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;

public class NessieModelIceberg {
  private NessieModelIceberg() {}

  public static IcebergSortOrder nessieSortDefinitionToIceberg(
      NessieSortDefinition sortDefinition, Function<UUID, NessieField> fieldById) {
    List<IcebergSortField> fields =
        sortDefinition.columns().stream()
            .map(
                f -> {
                  var nessieField = fieldById.apply(f.sourceFieldId());
                  checkState(
                      nessieField != null,
                      "Nessie field with ID %s not found for sort transform spec %s",
                      f.sourceFieldId(),
                      f.transformSpec());
                  return IcebergSortField.sortField(
                      nessieTransformToIceberg(f.transformSpec()).toString(),
                      nessieField.icebergId(),
                      f.direction(),
                      f.nullOrder());
                })
            .collect(Collectors.toList());
    return IcebergSortOrder.sortOrder(sortDefinition.icebergSortOrderId(), fields);
  }

  public static NessieSortDefinition icebergSortOrderToNessie(
      IcebergSortOrder sortOrder, Map<Integer, NessieField> icebergFields) {
    List<NessieSortField> fields =
        sortOrder.fields().stream()
            .map(
                f -> {
                  NessieFieldTransform transform =
                      icebergTransformToNessie(IcebergTransform.fromString(f.transform()));
                  NessieField sourceField = icebergFields.get(f.sourceId());
                  checkArgument(
                      sourceField != null,
                      "Iceberg field with ID %s for sort order with ID %s and transform '%s' does not exist",
                      f.sourceId(),
                      sortOrder.orderId(),
                      f.transform());
                  return NessieSortField.builder()
                      .sourceFieldId(sourceField.id())
                      .nullOrder(f.nullOrder())
                      .direction(f.direction())
                      .transformSpec(transform)
                      .type(transform.transformedType(sourceField.type()))
                      .build();
                })
            .collect(Collectors.toList());

    return NessieSortDefinition.nessieSortDefinition(fields, sortOrder.orderId());
  }

  public static IcebergPartitionSpec nessiePartitionDefinitionToIceberg(
      NessiePartitionDefinition partitionDefinition, Function<UUID, NessieField> fieldById) {
    List<IcebergPartitionField> fields =
        partitionDefinition.fields().stream()
            .map(
                f -> {
                  var nessieField = fieldById.apply(f.sourceFieldId());
                  checkState(
                      nessieField != null,
                      "Nessie field with ID %s not found for partition field with ID %s (%s, '%s') with transform '%s'",
                      f.sourceFieldId(),
                      f.id(),
                      f.icebergId(),
                      f.name(),
                      f.transformSpec());
                  return partitionField(
                      f.name(),
                      nessieTransformToIceberg(f.transformSpec()).toString(),
                      nessieField.icebergId(),
                      f.icebergId());
                })
            .collect(Collectors.toList());
    return IcebergPartitionSpec.partitionSpec(partitionDefinition.icebergId(), fields);
  }

  static IcebergTransform nessieTransformToIceberg(NessieFieldTransform nessieFieldTransform) {
    // TODO check if we keep the same syntax or do something else
    return IcebergTransform.fromString(nessieFieldTransform.toString());
  }

  static NessieFieldTransform icebergTransformToNessie(IcebergTransform icebergTransform) {
    // TODO check if we keep the same syntax or do something else
    return NessieFieldTransform.fromString(icebergTransform.toString());
  }

  public static NessiePartitionDefinition icebergPartitionSpecToNessie(
      IcebergPartitionSpec partitionSpec,
      Map<Integer, NessiePartitionField> icebergPartitionFields,
      Map<Integer, NessieField> icebergFields) {
    List<NessiePartitionField> fields =
        partitionSpec.fields().stream()
            .map(
                p -> {
                  NessieFieldTransform transform =
                      icebergTransformToNessie(IcebergTransform.fromString(p.transform()));
                  NessieField sourceField = icebergFields.get(p.sourceId());
                  checkArgument(
                      sourceField != null,
                      "Source field with ID %s not found for partition spec with ID %s and transform '%s'",
                      p.sourceId(),
                      partitionSpec.specId(),
                      p.transform());
                  NessiePartitionField existing = icebergPartitionFields.get(p.fieldId());
                  UUID id = existing != null ? existing.id() : UUID.randomUUID();
                  NessiePartitionField partitionField =
                      NessiePartitionField.nessiePartitionField(
                          id,
                          sourceField.id(),
                          p.name(),
                          transform.transformedType(sourceField.type()),
                          transform,
                          p.fieldId());
                  icebergPartitionFields.put(p.fieldId(), partitionField);
                  return partitionField;
                })
            .collect(Collectors.toList());

    return NessiePartitionDefinition.nessiePartitionDefinition(fields, partitionSpec.specId());
  }

  public static IcebergSchema nessieSchemaToIcebergSchema(NessieSchema schema) {
    Map<UUID, Integer> idMap = new HashMap<>();
    collectNessieToIcebergFieldMappings(schema.struct(), idMap);

    List<IcebergNestedField> fields =
        schema.struct().fields().stream()
            .map(
                f -> {
                  return IcebergNestedField.nestedField(
                      f.icebergId(),
                      f.name(),
                      !f.nullable(),
                      nessieTypeToIcebergType(f.type()),
                      f.doc());
                })
            .collect(Collectors.toList());

    List<Integer> identifierFieldIds =
        schema.identifierFields().stream().map(idMap::get).collect(Collectors.toList());

    return IcebergSchema.schema(schema.icebergId(), identifierFieldIds, fields);
  }

  private static void collectNessieToIcebergFieldMappings(
      NessieStruct struct, Map<UUID, Integer> idMap) {
    for (NessieField field : struct.fields()) {
      collectNessieToIcebergFieldMappings(field, idMap);
    }
  }

  private static void collectNessieToIcebergFieldMappings(
      NessieField field, Map<UUID, Integer> idMap) {
    if (field.icebergId() != NessieField.NO_COLUMN_ID) {
      idMap.put(field.id(), field.icebergId());
      var type = field.type();
      if (requireNonNull(type.type()) == NessieType.STRUCT) {
        var structType = (NessieStructTypeSpec) type;
        collectNessieToIcebergFieldMappings(structType.struct(), idMap);
      }
    }
  }

  public static NessieSchema icebergSchemaToNessieSchema(
      IcebergSchema schema, Map<Integer, NessieField> icebergFields) {
    NessieStruct struct =
        icebergStructFieldsToNessie(
            schema.fields(),
            null, // TODO
            icebergFields);

    List<UUID> identifierFieldIds =
        schema.identifierFieldIds().stream()
            .map(icebergFields::get)
            .map(NessieField::id)
            .collect(Collectors.toList());

    return NessieSchema.nessieSchema(struct, schema.schemaId(), identifierFieldIds);
  }

  public static IcebergType nessieTypeToIcebergType(NessieTypeSpec type) {
    switch (type.type()) {
      case BOOLEAN:
        return IcebergType.booleanType();
      case INT:
        return IcebergType.integerType();
      case BIGINT:
        return IcebergType.longType();
      case FLOAT:
        return IcebergType.floatType();
      case DOUBLE:
        return IcebergType.doubleType();
      case UUID:
        return IcebergType.uuidType();
      case STRING:
        return IcebergType.stringType();
      case DATE:
        return IcebergType.dateType();
      case TIME:
        NessieTimeTypeSpec time = (NessieTimeTypeSpec) type;
        if (time.precision() != DEFAULT_TIME_PRECISION || time.withTimeZone()) {
          throw new IllegalArgumentException("Data type not supported in Iceberg: " + type);
        }
        return IcebergType.timeType();
      case TIMESTAMP:
        NessieTimestampTypeSpec timestamp = (NessieTimestampTypeSpec) type;
        if (timestamp.precision() != DEFAULT_TIME_PRECISION) {
          throw new IllegalArgumentException("Data type not supported in Iceberg: " + type);
        }
        return timestamp.withTimeZone()
            ? IcebergType.timestamptzType()
            : IcebergType.timestampType();
      case BINARY:
        return IcebergType.binaryType();
      case DECIMAL:
        NessieDecimalTypeSpec decimal = (NessieDecimalTypeSpec) type;
        return IcebergType.decimalType(decimal.precision(), decimal.scale());
      case FIXED:
        NessieFixedTypeSpec fixed = (NessieFixedTypeSpec) type;
        return IcebergType.fixedType(fixed.length());
      case LIST:
        NessieListTypeSpec list = (NessieListTypeSpec) type;
        return IcebergType.listType(
            list.icebergElementFieldId(),
            nessieTypeToIcebergType(list.elementType()),
            !list.elementsNullable());
      case MAP:
        NessieMapTypeSpec map = (NessieMapTypeSpec) type;
        return IcebergType.mapType(
            map.icebergKeyFieldId(),
            nessieTypeToIcebergType(map.keyType()),
            map.icebergValueFieldId(),
            nessieTypeToIcebergType(map.valueType()),
            !map.valuesNullable());
      case STRUCT:
        NessieStructTypeSpec struct = (NessieStructTypeSpec) type;
        return IcebergType.structType(
            struct.struct().fields().stream()
                .map(NessieModelIceberg::nessieFieldToIceberg)
                .collect(Collectors.toList()),
            struct.struct().icebergRecordName());
      case INTERVAL:
      case TINYINT:
      case SMALLINT:
      default:
        throw new IllegalArgumentException("Type unsupported in Iceberg: " + type);
    }
  }

  private static IcebergNestedField nessieFieldToIceberg(NessieField nessieField) {
    return IcebergNestedField.nestedField(
        nessieField.icebergId(),
        nessieField.name(),
        !nessieField.nullable(),
        nessieTypeToIcebergType(nessieField.type()),
        nessieField.doc());
  }

  static NessieTypeSpec icebergTypeToNessieType(
      IcebergType type, Map<Integer, NessieField> icebergFields) {
    switch (type.type()) {
      case IcebergType.TYPE_TIMESTAMP_TZ:
        return NessieType.timestampType(true);
      case IcebergType.TYPE_TIMESTAMP:
        return NessieType.timestampType(false);
      case IcebergType.TYPE_BOOLEAN:
        return NessieType.booleanType();
      case IcebergType.TYPE_UUID:
        return NessieType.uuidType();
      case IcebergType.TYPE_INT:
        return NessieType.intType();
      case IcebergType.TYPE_LONG:
        return NessieType.bigintType();
      case IcebergType.TYPE_FLOAT:
        return NessieType.floatType();
      case IcebergType.TYPE_DOUBLE:
        return NessieType.doubleType();
      case IcebergType.TYPE_DATE:
        return NessieType.dateType();
      case IcebergType.TYPE_TIME:
        return NessieType.timeType();
      case IcebergType.TYPE_STRING:
        return NessieType.stringType();
      case IcebergType.TYPE_BINARY:
        return NessieType.binaryType();
      default:
        break;
    }

    if (type instanceof IcebergDecimalType) {
      IcebergDecimalType f = (IcebergDecimalType) type;
      return NessieType.decimalType(f.scale(), f.precision());
    }
    if (type instanceof IcebergFixedType) {
      IcebergFixedType f = (IcebergFixedType) type;
      return NessieType.fixedType(f.length());
    }
    if (type instanceof IcebergStructType) {
      IcebergStructType s = (IcebergStructType) type;
      return NessieType.structType(
          icebergStructFieldsToNessie(s.fields(), s.avroRecordName(), icebergFields));
    }
    if (type instanceof IcebergMapType) {
      IcebergMapType m = (IcebergMapType) type;
      return NessieType.mapType(
          icebergTypeToNessieType(m.key(), icebergFields),
          m.keyId(),
          icebergTypeToNessieType(m.value(), icebergFields),
          m.valueId(),
          !m.valueRequired());
    }
    if (type instanceof IcebergListType) {
      IcebergListType l = (IcebergListType) type;
      return NessieType.listType(
          icebergTypeToNessieType(l.element(), icebergFields), l.elementId(), !l.elementRequired());
    }

    throw new IllegalArgumentException("Unsupported Iceberg type " + type);
  }

  static NessieStruct icebergStructFieldsToNessie(
      List<IcebergNestedField> fields, String recordName, Map<Integer, NessieField> icebergFields) {
    return NessieStruct.nessieStruct(
        fields.stream()
            .map(
                f -> {
                  NessieTypeSpec nessieType = icebergTypeToNessieType(f.type(), icebergFields);

                  NessieField existing = icebergFields.get(f.id());
                  UUID id = existing != null ? existing.id() : UUID.randomUUID();
                  // TODO should we check whether the type is convertible, if different?

                  NessieField field =
                      NessieField.builder()
                          .id(id)
                          .name(f.name())
                          .type(nessieType)
                          .nullable(!f.required())
                          .icebergId(f.id())
                          .doc(f.doc())
                          .build();
                  icebergFields.put(f.id(), field);
                  return field;
                })
            .collect(Collectors.toList()),
        recordName);
  }

  public static NessieViewSnapshot icebergViewSnapshotToNessie(
      NessieId snapshotId,
      NessieViewSnapshot previous,
      NessieView view,
      IcebergViewMetadata iceberg) {
    NessieViewSnapshot.Builder snapshot = NessieViewSnapshot.builder().id(snapshotId);
    if (previous != null) {
      snapshot.from(previous);

      String previousLocation = previous.icebergLocation();
      if (previousLocation != null && !previousLocation.equals(iceberg.location())) {
        snapshot.addAdditionalKnownLocation(previous.icebergLocation());
      }
    }

    int formatVersion = iceberg.formatVersion();
    snapshot
        .entity(view)
        .icebergFormatVersion(formatVersion)
        .icebergLocation(iceberg.location())
        .properties(iceberg.properties())
        .icebergLocation(iceberg.location());

    IcebergViewVersion currentVersion =
        iceberg.versions().stream()
            .filter(v -> v.versionId() == iceberg.currentVersionId())
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "IcebergViewMetadata has no current version view-version"));
    int currentSchemaId = currentVersion.schemaId();

    Map<Integer, NessieField> icebergFields;
    if (previous != null) {
      icebergFields = collectFieldsByIcebergId(previous.schemas());
    } else {
      icebergFields = new HashMap<>();
    }

    for (IcebergSchema schema : iceberg.schemas()) {
      NessieSchema nessieSchema = icebergSchemaToNessieSchema(schema, icebergFields);

      snapshot.addSchemas(nessieSchema);
      if (schema.schemaId() == currentSchemaId) {
        snapshot.currentSchemaId(nessieSchema.id());
      }
    }

    Instant lastUpdatedTimestamp = Instant.ofEpochMilli(currentVersion.timestampMs());
    snapshot
        .icebergCurrentVersionId(currentVersion.versionId())
        .icebergDefaultCatalog(currentVersion.defaultCatalog())
        .icebergNamespace(currentVersion.defaultNamespace().levels())
        .icebergVersionSummary(currentVersion.summary())
        .snapshotCreatedTimestamp(now())
        .lastUpdatedTimestamp(lastUpdatedTimestamp);

    return snapshot.build();
  }

  public static NessieTableSnapshot icebergTableSnapshotToNessie(
      NessieId snapshotId,
      NessieTableSnapshot previous,
      NessieTable table,
      IcebergTableMetadata iceberg,
      Function<IcebergSnapshot, String> manifestListLocation) {
    NessieTableSnapshot.Builder snapshot = NessieTableSnapshot.builder().id(snapshotId);
    if (previous != null) {
      snapshot.from(previous);

      String previousLocation = previous.icebergLocation();
      if (previousLocation != null && !previousLocation.equals(iceberg.location())) {
        snapshot.addAdditionalKnownLocation(previous.icebergLocation());
      }
    }

    int formatVersion = iceberg.formatVersion();
    snapshot
        .entity(table)
        .icebergFormatVersion(formatVersion)
        .icebergLastSequenceNumber(iceberg.lastSequenceNumber())
        .icebergLocation(iceberg.location())
        .lastUpdatedTimestamp(Instant.ofEpochMilli(iceberg.lastUpdatedMs()))
        .icebergLastColumnId(iceberg.lastColumnId())
        .icebergLastPartitionId(iceberg.lastPartitionId());

    snapshot.properties(emptyMap());
    iceberg
        .properties()
        .forEach(
            (k, v) -> {
              if (!DERIVED_PROPERTIES.contains(k)) {
                snapshot.putProperty(k, v);
              }
            });

    int currentSchemaId =
        extractCurrentId(iceberg.currentSchemaId(), iceberg.schema(), IcebergSchema::schemaId);
    int defaultSpecId =
        extractCurrentId(iceberg.defaultSpecId(), null, IcebergPartitionSpec::specId);
    int defaultSortOrderId =
        extractCurrentId(iceberg.defaultSortOrderId(), null, IcebergSortOrder::orderId);

    Map<Integer, NessieField> icebergFields;
    Map<Integer, NessiePartitionField> icebergPartitionFields = new HashMap<>();
    if (previous != null) {
      icebergFields = collectFieldsByIcebergId(previous.schemas());
      for (NessiePartitionDefinition partitionDefinition : previous.partitionDefinitions()) {
        collectPartitionFields(partitionDefinition, icebergPartitionFields);
      }
    } else {
      icebergFields = new HashMap<>();
    }

    Map<Integer, NessieSchema> icebergSchemaIdToSchema = new HashMap<>();

    partsWithV1DefaultPart(iceberg.schemas(), iceberg.schema(), IcebergSchema::schemaId)
        .forEach(
            schema -> {
              boolean isCurrent = schema.schemaId() == currentSchemaId;

              NessieSchema nessieSchema = icebergSchemaToNessieSchema(schema, icebergFields);

              icebergSchemaIdToSchema.put(schema.schemaId(), nessieSchema);

              snapshot.addSchemas(nessieSchema);
              if (isCurrent) {
                snapshot.currentSchemaId(nessieSchema.id());
              }
            });

    IcebergPartitionSpec partitionSpecV1;
    if (formatVersion == 1 && defaultSpecId != -1 && !iceberg.partitionSpec().isEmpty()) {
      // TODO add test(s) for this case
      partitionSpecV1 =
          IcebergPartitionSpec.builder()
              .specId(defaultSpecId)
              .fields(iceberg.partitionSpec())
              .build();
    } else {
      partitionSpecV1 = null;
    }

    partsWithV1DefaultPart(iceberg.partitionSpecs(), partitionSpecV1, IcebergPartitionSpec::specId)
        .forEach(
            partitionSpec -> {
              // TODO add test(s) for this case
              boolean isCurrent = partitionSpec.specId() == defaultSpecId;

              NessiePartitionDefinition partitionDefinition =
                  icebergPartitionSpecToNessie(
                      partitionSpec, icebergPartitionFields, icebergFields);

              snapshot.addPartitionDefinitions(partitionDefinition);
              if (isCurrent) {
                snapshot.currentPartitionDefinitionId(partitionDefinition.id());
              }
            });

    partsWithV1DefaultPart(iceberg.sortOrders(), null, IcebergSortOrder::orderId)
        .forEach(
            sortOrder -> {
              // TODO add test(s) for this case
              boolean isCurrent = sortOrder.orderId() == defaultSortOrderId;

              NessieSortDefinition sortDefinition =
                  icebergSortOrderToNessie(sortOrder, icebergFields);

              snapshot.addSortDefinitions(sortDefinition);
              if (isCurrent) {
                snapshot.currentSortDefinitionId(sortDefinition.id());
              }
            });

    iceberg
        .currentSnapshot()
        .ifPresent(
            currentSnapshot -> {
              snapshot.icebergSnapshotId(currentSnapshot.snapshotId());
              currentSnapshot
                  .parentSnapshotId(); // TODO Can we leave this unset, as we do not return previous
              // Iceberg
              // snapshots??
              Integer schemaId = currentSnapshot.schemaId();
              if (schemaId != null) {
                // TODO this overwrites the "current schema ID" with the schema ID of the current
                //  snapshot. Is this okay??
                NessieSchema currentSchema = icebergSchemaIdToSchema.get(schemaId);
                if (currentSchema != null) {
                  snapshot.currentSchemaId(currentSchema.id());
                }
              }

              String listLocation = manifestListLocation.apply(currentSnapshot);

              snapshot
                  .icebergSnapshotSequenceNumber(currentSnapshot.sequenceNumber())
                  .snapshotCreatedTimestamp(Instant.ofEpochMilli(currentSnapshot.timestampMs()))
                  .icebergSnapshotSummary(currentSnapshot.summary())
                  .icebergManifestListLocation(listLocation)
                  .icebergManifestFileLocations(currentSnapshot.manifests());

              if (listLocation == null) {
                // Empty manifest-group if there are no manifest files.
              }

              currentSnapshot.manifestList(); // TODO
              currentSnapshot.manifests(); // TODO
            });

    for (IcebergSnapshotLogEntry logEntry : iceberg.snapshotLog()) {
      // TODO ??
      logEntry.snapshotId();
      logEntry.timestampMs();
    }

    for (IcebergStatisticsFile statisticsFile : iceberg.statistics()) {
      if (statisticsFile.snapshotId() == iceberg.currentSnapshotIdAsLong()) {
        snapshot.addStatisticsFile(icebergStatisticsFileToNessie(statisticsFile));
      }
    }

    for (IcebergPartitionStatisticsFile partitionStatistic : iceberg.partitionStatistics()) {
      if (partitionStatistic.snapshotId() == iceberg.currentSnapshotIdAsLong()) {
        snapshot.addPartitionStatisticsFile(
            icebergPartitionStatisticsFileToNessie(partitionStatistic));
      }
    }

    for (IcebergHistoryEntry historyEntry : iceberg.metadataLog()) {
      // TODO needed??
      // TODO add test(s) for this case??
      historyEntry.metadataFile();
      historyEntry.timestampMs();
    }

    iceberg.refs(); // TODO ??

    return snapshot.build();
  }

  public static NessieStatisticsFile icebergStatisticsFileToNessie(
      IcebergStatisticsFile icebergStatisticsFile) {
    return statisticsFile(
        icebergStatisticsFile.statisticsPath(),
        icebergStatisticsFile.fileSizeInBytes(),
        icebergStatisticsFile.fileFooterSizeInBytes(),
        icebergStatisticsFile.blobMetadata().stream()
            .map(
                blobMetadata ->
                    NessieIcebergBlobMetadata.blobMetadata(
                        blobMetadata.type(),
                        blobMetadata.sequenceNumber(),
                        blobMetadata.fields(),
                        blobMetadata.properties()))
            .collect(Collectors.toList()));
  }

  public static NessiePartitionStatisticsFile icebergPartitionStatisticsFileToNessie(
      IcebergPartitionStatisticsFile icebergPartitionStatisticsFile) {
    return partitionStatisticsFile(
        icebergPartitionStatisticsFile.statisticsPath(),
        icebergPartitionStatisticsFile.fileSizeInBytes());
  }

  /**
   * Collects all fields, top-level and nested in structs, from the given schema into the given map.
   *
   * <p>Similar to {@link #collectFieldsByIcebergId(NessieStruct, Map)}.
   */
  public static void collectFieldsByNessieId(
      NessieSchema schema, Map<UUID, NessieField> targetFieldMap) {
    collectFieldsByNessieId(schema.struct(), targetFieldMap);
  }

  /**
   * Collects all fields, top-level and nested in structs, from the given struct into the given map.
   *
   * <p>Does <em>not</em> return fields inside lists or maps, as those cannot be referred from
   * partition-specs or sort-orders.
   *
   * <p>Similar to {@link #collectFieldsByIcebergId(NessieStruct, Map)}.
   */
  public static void collectFieldsByNessieId(
      NessieStruct struct, Map<UUID, NessieField> targetFieldMap) {
    for (var field : struct.fields()) {
      targetFieldMap.put(field.id(), field);
      var type = field.type();
      if (requireNonNull(type.type()) == NessieType.STRUCT) {
        collectFieldsByNessieId(((NessieStructTypeSpec) type).struct(), targetFieldMap);
      }
    }
  }

  /**
   * Collects all fields, top-level and nested in structs, from the given schemas into the given
   * map.
   *
   * <p>Does <em>not</em> return fields inside lists or maps, as those cannot be referred from
   * partition-specs or sort-orders.
   *
   * <p>Similar to {@link #collectFieldsByNessieId(NessieStruct, Map)}.
   */
  @VisibleForTesting
  static Map<Integer, NessieField> collectFieldsByIcebergId(List<NessieSchema> schemas) {
    Map<Integer, NessieField> icebergFields = new HashMap<>();
    for (NessieSchema schema : schemas) {
      collectFieldsByIcebergId(schema.struct(), icebergFields);
    }
    return icebergFields;
  }

  private static void collectFieldsByIcebergId(
      NessieStruct struct, Map<Integer, NessieField> icebergFields) {
    for (NessieField field : struct.fields()) {
      collectFieldsByIcebergId(field, icebergFields);
    }
  }

  private static void collectFieldsByIcebergId(
      NessieField field, Map<Integer, NessieField> icebergFields) {
    if (field.icebergId() != NessieField.NO_COLUMN_ID) {
      icebergFields.put(field.icebergId(), field);
      var type = field.type();
      if (requireNonNull(type.type()) == NessieType.STRUCT) {
        var structType = (NessieStructTypeSpec) type;
        collectFieldsByIcebergId(structType.struct(), icebergFields);
      }
    }
  }

  private static void collectPartitionFields(
      NessiePartitionDefinition partitionDefinition,
      Map<Integer, NessiePartitionField> icebergPartitionFields) {
    for (NessiePartitionField field : partitionDefinition.fields()) {
      if (field.icebergId() != NessiePartitionField.NO_FIELD_ID) {
        icebergPartitionFields.put(field.icebergId(), field);
      }
    }
  }

  static <T> int extractCurrentId(Integer fromTableMetadata, T single, ToIntFunction<T> extractId) {
    if (fromTableMetadata != null) {
      return fromTableMetadata;
    }
    if (single != null) {
      return extractId.applyAsInt(single);
    }
    // no current schema/spec/sort-order, return an ID value that's definitely not present
    return -1;
  }

  /**
   * Iceberg format spec allows the singleton fields {@code schema} referencing the default/current
   * schema, in addition to the lists containing these parts.
   *
   * <p>This function optionally concatenates the singleton to a given list, both parameters are
   * nullable.
   */
  private static <T> Stream<T> partsWithV1DefaultPart(
      List<T> list, T single, ToIntFunction<T> extractId) {
    // TODO add test(s) for this function
    if (single == null) {
      // no 'single', can short-cut here
      return list != null ? list.stream() : Stream.empty();
    }
    if (list == null || list.isEmpty()) {
      // no/empty 'list', return a singleton
      return Stream.of(single);
    }

    int singleId = extractId.applyAsInt(single);
    for (T t : list) {
      int id = extractId.applyAsInt(t);
      if (id == singleId) {
        // 'single' is in 'list', can short-cut here
        return list.stream();
      }
    }
    return Stream.concat(Stream.of(single), list.stream());
  }

  public static int safeUnbox(Integer value, int defaultValue) {
    return value != null ? value : defaultValue;
  }

  public static long safeUnbox(Long value, long defaultValue) {
    return value != null ? value : defaultValue;
  }

  /**
   * Returns a unique base location for the given key.
   *
   * <p>Different tables or views with same name can exist across references in Nessie. To avoid
   * sharing the same path between two tables or views with the same name, we randomize it by
   * appending a uuid suffix to the path.
   *
   * <p>Also: we deliberately ignore the TableProperties.WRITE_METADATA_LOCATION property here.
   */
  public static String icebergNewEntityBaseLocation(String baseLocation) {
    // FIXME escape or remove forbidden chars, cf. #8524
    return baseLocation + "_" + randomUUID();
  }

  private static String concatLocation(String location, String key) {
    return location.endsWith("/") ? location + key : location + "/" + key;
  }

  /** Returns the table or view metadata JSON location. */
  public static String icebergMetadataJsonLocation(String baseLocation) {
    return String.format("%s/metadata/00000-%s.metadata.json", baseLocation, randomUUID());
  }

  public static NessieTableSnapshot newIcebergTableSnapshot(String icebergUuid) {
    // Use a transient ID for this snapshot because it is not stored as a Nessie snapshot,
    // but is used only to generate Iceberg snapshot data. A proper Nessie snapshot will
    // be created later, when the Iceberg snapshot is loaded after a successful Nessie commit.
    NessieId nessieId = transientNessieId();
    Instant now = now();
    NessieTable nessieTable =
        NessieTable.builder()
            .tableFormat(ICEBERG)
            .icebergUuid(icebergUuid)
            .nessieContentId(randomUUID().toString()) // Not used / redefined after commit
            .createdTimestamp(now)
            .build();
    return NessieTableSnapshot.builder()
        .id(nessieId)
        .entity(nessieTable)
        .lastUpdatedTimestamp(now)
        .icebergLastSequenceNumber(IcebergTableMetadata.INITIAL_SEQUENCE_NUMBER)
        .icebergLastPartitionId(INITIAL_PARTITION_ID)
        .build();
  }

  public static NessieViewSnapshot newIcebergViewSnapshot(String icebergUuid) {
    // Use a transient ID for this snapshot because it is not stored as a Nessie snapshot,
    // but is used only to generate Iceberg snapshot data. A proper Nessie snapshot will
    // be created later, when the Iceberg snapshot is loaded after a successful Nessie commit.
    NessieId nessieId = transientNessieId();
    Instant now = now();
    NessieView nessieView =
        NessieView.builder()
            .tableFormat(ICEBERG)
            .icebergUuid(icebergUuid)
            .nessieContentId(randomUUID().toString()) // Not used / redefined after commit
            .createdTimestamp(now)
            .build();
    return NessieViewSnapshot.builder()
        .id(nessieId)
        .entity(nessieView)
        .lastUpdatedTimestamp(now)
        .build();
  }

  public static void upgradeFormatVersion(
      int formatVersion,
      NessieEntitySnapshot<?> snapshot,
      NessieEntitySnapshot.Builder<?> builder) {
    Integer currentVersion = snapshot.icebergFormatVersion();
    if (currentVersion == null) {
      builder.icebergFormatVersion(formatVersion);
    } else if (formatVersion != currentVersion) {
      throw new UnsupportedOperationException(
          "Implement format version update, current version is "
              + currentVersion
              + ", requested format version is "
              + formatVersion);
    }
  }

  public static IcebergViewRepresentation nessieViewRepresentationToIceberg(
      NessieViewRepresentation viewRepresentation) {
    if (viewRepresentation instanceof NessieViewSQLRepresentation) {
      NessieViewSQLRepresentation sql = (NessieViewSQLRepresentation) viewRepresentation;
      return IcebergSQLViewRepresentation.icebergSqlViewRepresentation(sql.sql(), sql.dialect());
    }
    throw new IllegalArgumentException("Unsupported view representation: " + viewRepresentation);
  }

  public static NessieViewRepresentation icebergViewRepresentationToNessie(
      IcebergViewRepresentation viewRepresentation) {
    if (viewRepresentation instanceof IcebergSQLViewRepresentation) {
      IcebergSQLViewRepresentation sql = (IcebergSQLViewRepresentation) viewRepresentation;
      return nessieViewSQLRepresentation(sql.sql(), sql.dialect());
    }
    throw new IllegalArgumentException("Unsupported view representation: " + viewRepresentation);
  }

  public static IcebergViewMetadata nessieViewSnapshotToIceberg(
      NessieViewSnapshot nessie,
      Optional<IcebergSpec> requestedSpecVersion,
      Consumer<Map<String, String>> viewPropertiesTweak) {
    NessieView entity = nessie.entity();

    checkArgument(entity.tableFormat() == TableFormat.ICEBERG, "Not an Iceberg view.");

    IcebergSpec spec =
        requestedSpecVersion.orElse(
            IcebergSpec.forVersion(safeUnbox(nessie.icebergFormatVersion(), 2)));

    long currentVersionId = safeUnbox(nessie.icebergCurrentVersionId(), NO_SNAPSHOT_ID);

    Map<String, String> properties = new HashMap<>(nessie.properties());
    viewPropertiesTweak.accept(properties);

    IcebergViewMetadata.Builder metadata =
        IcebergViewMetadata.builder()
            .viewUuid(entity.icebergUuid())
            .formatVersion(spec.version())
            .currentVersionId(currentVersionId)
            .location(nessie.icebergLocation())
            .properties(properties);

    int currentSchemaId = INITIAL_SCHEMA_ID;
    for (NessieSchema schema : nessie.schemas()) {
      IcebergSchema iceberg = nessieSchemaToIcebergSchema(schema);
      metadata.addSchemas(iceberg);
      if (schema.id().equals(nessie.currentSchemaId())) {
        currentSchemaId = schema.icebergId();
      }
    }

    if (currentVersionId != NO_SNAPSHOT_ID) {
      long timestampMs = nessie.snapshotCreatedTimestamp().toEpochMilli();
      IcebergViewVersion.Builder version =
          IcebergViewVersion.builder()
              .versionId(currentVersionId)
              .schemaId(currentSchemaId)
              .defaultCatalog(nessie.icebergDefaultCatalog())
              .defaultNamespace(icebergNamespace(nessie.icebergNamespace()))
              .summary(nessie.icebergVersionSummary())
              .timestampMs(timestampMs);

      nessie.representations().stream()
          .map(NessieModelIceberg::nessieViewRepresentationToIceberg)
          .forEach(version::addRepresentation);

      metadata
          .addVersion(version.build())
          .addVersionLog(icebergViewHistoryEntry(timestampMs, currentVersionId));
    }

    return metadata.build();
  }

  public static IcebergTableMetadata nessieTableSnapshotToIceberg(
      NessieTableSnapshot nessie,
      Optional<IcebergSpec> requestedSpecVersion,
      Consumer<Map<String, String>> tablePropertiesTweak) {
    NessieTable entity = nessie.entity();

    checkArgument(entity.tableFormat() == TableFormat.ICEBERG, "Not an Iceberg table.");

    IcebergSpec spec =
        requestedSpecVersion.orElse(
            IcebergSpec.forVersion(safeUnbox(nessie.icebergFormatVersion(), 2)));

    long snapshotId = safeUnbox(nessie.icebergSnapshotId(), NO_SNAPSHOT_ID);

    Map<String, String> properties = new HashMap<>(nessie.properties());
    tablePropertiesTweak.accept(properties);

    IcebergTableMetadata.Builder metadata =
        IcebergTableMetadata.builder()
            .tableUuid(entity.icebergUuid())
            .formatVersion(spec.version())
            .currentSnapshotId(snapshotId)
            .lastColumnId(safeUnbox(nessie.icebergLastColumnId(), INITIAL_COLUMN_ID))
            .lastPartitionId(safeUnbox(nessie.icebergLastPartitionId(), INITIAL_PARTITION_ID))
            .lastSequenceNumber(nessie.icebergLastSequenceNumber())
            .location(nessie.icebergLocation())
            .properties(properties)
            .lastUpdatedMs(nessie.lastUpdatedTimestamp().toEpochMilli());

    switch (spec) {
      case V1:
        metadata.lastSequenceNumber(0L);
        break;
      case V2:
        metadata.lastSequenceNumber(nessie.icebergLastSequenceNumber());
        break;
      default:
        throw new IllegalArgumentException(spec.toString());
    }

    int currentSchemaId = INITIAL_SCHEMA_ID;
    var allSchemasFieldsById = new HashMap<UUID, NessieField>();
    for (NessieSchema schema : nessie.schemas()) {
      collectFieldsByNessieId(schema, allSchemasFieldsById);
      IcebergSchema iceberg = nessieSchemaToIcebergSchema(schema);
      metadata.addSchemas(iceberg);
      if (schema.id().equals(nessie.currentSchemaId())) {
        currentSchemaId = schema.icebergId();
        if (spec.version() == 1) {
          metadata.schema(iceberg);
        }
      }
    }
    metadata.currentSchemaId(currentSchemaId);

    int defaultSpecId = INITIAL_SPEC_ID;
    for (NessiePartitionDefinition partitionDefinition : nessie.partitionDefinitions()) {
      // TODO add test(s) for this
      IcebergPartitionSpec iceberg =
          nessiePartitionDefinitionToIceberg(partitionDefinition, allSchemasFieldsById::get);
      metadata.addPartitionSpecs(iceberg);
      if (partitionDefinition.id().equals(nessie.currentPartitionDefinitionId())) {
        defaultSpecId = partitionDefinition.icebergId();
        if (spec.version() == 1) {
          metadata.partitionSpec(iceberg.fields());
        }
      }
    }
    metadata.defaultSpecId(defaultSpecId);

    int defaultSortOrderId = INITIAL_SORT_ORDER_ID;
    for (NessieSortDefinition sortDefinition : nessie.sortDefinitions()) {
      // TODO add test(s) for this
      IcebergSortOrder iceberg =
          nessieSortDefinitionToIceberg(sortDefinition, allSchemasFieldsById::get);
      metadata.addSortOrders(iceberg);
      if (sortDefinition.id().equals(nessie.currentSortDefinitionId())) {
        defaultSortOrderId = sortDefinition.icebergSortOrderId();
      }
    }
    metadata.defaultSortOrderId(defaultSortOrderId);

    if (snapshotId != NO_SNAPSHOT_ID) {
      long timestampMs = nessie.snapshotCreatedTimestamp().toEpochMilli();
      String manifestListLocation = nessie.icebergManifestListLocation();
      // Only populate the `manifests` field for Iceberg spec v1 if the `manifest-list` location is
      // _not_ specified.
      List<String> manifestsLocations =
          spec == IcebergSpec.V1 && (manifestListLocation == null || manifestListLocation.isEmpty())
              ? nessie.icebergManifestFileLocations()
              : emptyList();

      IcebergSnapshot.Builder snapshot =
          IcebergSnapshot.builder()
              .snapshotId(snapshotId)
              .schemaId(currentSchemaId) // TODO is this fine?
              .manifests(manifestsLocations)
              .manifestList(manifestListLocation)
              .summary(nessie.icebergSnapshotSummary())
              .timestampMs(timestampMs)
              .sequenceNumber(nessie.icebergSnapshotSequenceNumber());
      metadata.addSnapshots(snapshot.build());

      metadata.putRef(
          "main", IcebergSnapshotRef.builder().snapshotId(snapshotId).type("branch").build());

      metadata.addSnapshotLog(
          IcebergSnapshotLogEntry.builder()
              .snapshotId(snapshotId)
              .timestampMs(timestampMs)
              .build());
    }

    for (NessieStatisticsFile statisticsFile : nessie.statisticsFiles()) {
      metadata.addStatistic(
          IcebergStatisticsFile.statisticsFile(
              snapshotId,
              statisticsFile.statisticsPath(),
              statisticsFile.fileSizeInBytes(),
              statisticsFile.fileFooterSizeInBytes(),
              statisticsFile.blobMetadata().stream()
                  .map(
                      blobMetadata ->
                          IcebergBlobMetadata.blobMetadata(
                              blobMetadata.type(),
                              snapshotId,
                              blobMetadata.sequenceNumber(),
                              blobMetadata.fields(),
                              blobMetadata.properties()))
                  .collect(Collectors.toList())));
    }

    for (NessiePartitionStatisticsFile partitionStatisticsFile :
        nessie.partitionStatisticsFiles()) {
      metadata.addPartitionStatistic(
          IcebergPartitionStatisticsFile.partitionStatisticsFile(
              snapshotId,
              partitionStatisticsFile.statisticsPath(),
              partitionStatisticsFile.fileSizeInBytes()));
    }

    //    metadata.addMetadataLog();
    //    metadata.addSnapshotLog();

    return metadata.build();
  }

  public static void assignUUID(AssignUUID u, NessieEntitySnapshot<?> snapshot) {
    String uuid = u.uuid();
    Preconditions.checkArgument(uuid != null, "Null entity UUID is not permitted.");
    Preconditions.checkArgument(
        uuid.equals(requireNonNull(snapshot.entity().icebergUuid())),
        "UUID mismatch: assigned: %s, new: %s",
        snapshot.entity().icebergUuid(),
        uuid);
  }

  public static void setLocation(SetLocation u, NessieEntitySnapshot.Builder<?> builder) {
    builder.icebergLocation(u.location());
  }

  public static void setProperties(
      SetProperties u, NessieEntitySnapshot<?> snapshot, NessieEntitySnapshot.Builder<?> builder) {
    Map<String, String> properties = new HashMap<>(snapshot.properties());
    u.updates()
        .forEach(
            (k, v) -> {
              if (!RESERVED_PROPERTIES.contains(k)) {
                properties.put(k, v);
              }
            });
    builder.properties(properties);
  }

  public static void removeProperties(
      RemoveProperties u,
      NessieEntitySnapshot<?> snapshot,
      NessieEntitySnapshot.Builder<?> builder) {
    Map<String, String> properties = new HashMap<>(snapshot.properties());
    u.removals().forEach(properties::remove);
    builder.properties(properties);
  }

  public static void addSchema(AddSchema u, IcebergTableMetadataUpdateState state) {
    IcebergSchema schema = u.schema();

    checkArgument(schema.schemaId() >= -1, "Invalid schema-ID %s", u.schema().schemaId());
    NessieTableSnapshot snapshot = state.snapshot();

    // TODO check again, carefully, how exactly and when Iceberg does the the following ID
    //  reassignment. It feels wrong to only do this for the very first schema (aka for a new
    //  table).
    Map<Integer, Integer> remappedFieldIds = new HashMap<>();
    if (snapshot.schemas().isEmpty()) {
      // new table, assign fresh IDs (as Iceberg does)
      schema = icebergInitialSchema(schema, remappedFieldIds);
    }

    if (schema.schemaId() != -1) {
      checkArgument(
          !snapshot.schemaByIcebergId(schema.schemaId()).isPresent(),
          "A schema with ID %s already exists",
          schema.schemaId());
    }

    // Check if a similar schema exists, and if so then update the "last added schema ID"
    // accordingly for a following "set-current-schema -1".
    ReuseOrCreate<IcebergSchema> reuseOrCreate = reuseOrCreateNewSchemaId(schema, snapshot);
    schema = schema.withSchemaId(reuseOrCreate.id());
    if (reuseOrCreate.value().isPresent()) {
      state.schemaAdded(schema.schemaId());
      return;
    }

    checkArgument(
        !snapshot.schemaByIcebergId(schema.schemaId()).isPresent(),
        "A schema with ID %s already exists",
        schema.schemaId());

    Map<Integer, NessieField> icebergFields = collectFieldsByIcebergId(snapshot.schemas());

    NessieSchema nessieSchema = icebergSchemaToNessieSchema(schema, icebergFields);
    checkArgument(
        nessieSchema.icebergId() >= 0,
        "No Iceberg-ID for NessieSchema, retrieved via schema-ID %s",
        u.schema().schemaId());
    state.builder().addSchema(nessieSchema);

    state.remappedFields(remappedFieldIds);

    int schemaMaxFieldId = icebergSchemaMaxFieldId(schema);
    int newLastColumnId = Math.max(u.lastColumnId().orElse(0), schemaMaxFieldId);
    Integer lastColumnId = snapshot.icebergLastColumnId();
    if (lastColumnId == null || newLastColumnId > lastColumnId) {
      state.builder().icebergLastColumnId(newLastColumnId);
    }

    state.schemaAdded(schema.schemaId());
  }

  public static int icebergSchemaMaxFieldId(IcebergSchema schema) {
    Set<Integer> fieldIds = new HashSet<>();
    fieldIds.add(INITIAL_COLUMN_ID);
    int max = 0;
    for (IcebergNestedField field : schema.fields()) {
      max = Math.max(max, icebergSchemaMaxFieldId(field, fieldIds));
    }
    return max;
  }

  public static int icebergSchemaMaxFieldId(IcebergNestedField field, Set<Integer> fieldIds) {
    checkArgument(fieldIds.add(field.id()), "Duplicate field ID %d", field.id());
    return Math.max(field.id(), icebergTypeMaxFieldId(field.type(), fieldIds));
  }

  public static int icebergTypeMaxFieldId(IcebergType type, Set<Integer> fieldIds) {
    int max = 0;
    switch (type.type()) {
      case TYPE_STRUCT:
        IcebergStructType srcStruct = (IcebergStructType) type;
        for (IcebergNestedField f : srcStruct.fields()) {
          max = Math.max(max, icebergSchemaMaxFieldId(f, fieldIds));
        }
        break;
      case TYPE_LIST:
        IcebergListType srcList = (IcebergListType) type;
        max = Math.max(max, srcList.elementId());
        max = Math.max(max, icebergTypeMaxFieldId(srcList.element(), fieldIds));
        break;
      case TYPE_MAP:
        IcebergMapType srcMap = (IcebergMapType) type;
        checkArgument(fieldIds.add(srcMap.keyId()), "Duplicate field ID %d", srcMap.keyId());
        checkArgument(fieldIds.add(srcMap.valueId()), "Duplicate field ID %d", srcMap.valueId());
        max = Math.max(max, srcMap.keyId());
        max = Math.max(max, srcMap.valueId());
        max = Math.max(max, icebergTypeMaxFieldId(srcMap.key(), fieldIds));
        max = Math.max(max, icebergTypeMaxFieldId(srcMap.value(), fieldIds));
        break;
      default:
        break;
    }
    return max;
  }

  public static IcebergSchema icebergInitialSchema(
      IcebergSchema schema, Map<Integer, Integer> remappedFieldIds) {
    IcebergSchema.Builder newSchemaBuilder = IcebergSchema.builder().from(schema);
    newSchemaBuilder.schemaId(INITIAL_SCHEMA_ID).fields(emptyList());
    AtomicInteger nextId = new AtomicInteger();
    for (IcebergNestedField field : schema.fields()) {
      IcebergNestedField newField = icebergInitialSchemaField(field, nextId, remappedFieldIds);
      newSchemaBuilder.addField(newField);
    }
    return newSchemaBuilder.build();
  }

  private static IcebergNestedField icebergInitialSchemaField(
      IcebergNestedField field, AtomicInteger nextId, Map<Integer, Integer> remappedFieldIds) {
    int newFieldId = nextId.incrementAndGet();
    IcebergNestedField.Builder newField = IcebergNestedField.builder().from(field).id(newFieldId);
    remappedFieldIds.put(field.id(), newFieldId);
    IcebergType type = maybeRemapNested(field.type(), nextId, remappedFieldIds);
    return newField.type(type).build();
  }

  private static IcebergType maybeRemapNested(
      IcebergType type, AtomicInteger nextId, Map<Integer, Integer> remappedFieldIds) {
    switch (type.type()) {
      case TYPE_STRUCT:
        IcebergStructType srcStruct = (IcebergStructType) type;
        IcebergStructType.Builder struct =
            IcebergStructType.builder().from(srcStruct).fields(emptyList());
        for (IcebergNestedField f : srcStruct.fields()) {
          struct.addField(icebergInitialSchemaField(f, nextId, remappedFieldIds));
        }
        type = struct.build();
        break;
      case TYPE_LIST:
        IcebergListType srcList = (IcebergListType) type;
        IcebergListType.Builder list =
            IcebergListType.builder()
                .from(srcList)
                .elementId(nextId.incrementAndGet())
                .element(maybeRemapNested(srcList.element(), nextId, remappedFieldIds));
        type = list.build();
        break;
      case TYPE_MAP:
        IcebergMapType srcMap = (IcebergMapType) type;

        IcebergMapType.Builder map =
            IcebergMapType.builder()
                .from(srcMap)
                .keyId(nextId.incrementAndGet())
                .key(maybeRemapNested(srcMap.key(), nextId, remappedFieldIds))
                .valueId(nextId.incrementAndGet())
                .value(maybeRemapNested(srcMap.value(), nextId, remappedFieldIds));
        type = map.build();
        break;
      default:
        break;
    }
    return type;
  }

  public static void addSchema(AddSchema u, IcebergViewMetadataUpdateState state) {
    IcebergSchema schema = u.schema();

    checkArgument(schema.schemaId() >= -1, "Invalid schema-ID %s", u.schema().schemaId());
    NessieViewSnapshot snapshot = state.snapshot();

    if (schema.schemaId() != -1) {
      checkArgument(
          !snapshot.schemaByIcebergId(schema.schemaId()).isPresent(),
          "A schema with ID %s already exists",
          schema.schemaId());
    }

    // Check if a similar schema exists, and if so then update the "last added schema ID"
    // accordingly for a following "set-current-schema -1".
    ReuseOrCreate<IcebergSchema> reuseOrCreate = reuseOrCreateNewSchemaId(schema, snapshot);
    schema = schema.withSchemaId(reuseOrCreate.id());
    if (reuseOrCreate.value().isPresent()) {
      state.schemaAdded(schema.schemaId());
      return;
    }

    checkArgument(
        !snapshot.schemaByIcebergId(schema.schemaId()).isPresent(),
        "A schema with ID %s already exists",
        schema.schemaId());

    Map<Integer, NessieField> icebergFields = collectFieldsByIcebergId(snapshot.schemas());

    NessieSchema nessieSchema = icebergSchemaToNessieSchema(schema, icebergFields);
    checkArgument(
        nessieSchema.icebergId() >= 0,
        "No Iceberg-ID for NessieSchema, retrieved via schema-ID %s",
        u.schema().schemaId());
    state.builder().addSchema(nessieSchema);

    state.schemaAdded(schema.schemaId());
  }

  private static ReuseOrCreate<IcebergSchema> reuseOrCreateNewSchemaId(
      IcebergSchema newSchema, NessieEntitySnapshot<?> snapshot) {
    Iterator<IcebergSchema> schemas =
        snapshot.schemas().stream().map(NessieModelIceberg::nessieSchemaToIcebergSchema).iterator();

    // if the schema already exists, use its id; otherwise use the highest id + 1
    int newSchemaId = snapshot.currentSchemaObject().map(NessieSchema::icebergId).orElse(0);
    while (schemas.hasNext()) {
      IcebergSchema schema = schemas.next();
      if (schema.sameSchema(newSchema)) {
        return reuseOrCreate(schema.schemaId(), schema);
      } else if (schema.schemaId() >= newSchemaId) {
        newSchemaId = schema.schemaId() + 1;
      }
    }
    return reuseOrCreate(newSchemaId);
  }

  public static void setCurrentSchema(
      SetCurrentSchema setCurrentSchema,
      int lastAddedSchemaId,
      NessieEntitySnapshot<?> snapshot,
      NessieEntitySnapshot.Builder<?> builder) {
    int schemaId = setCurrentSchema.schemaId();
    if (schemaId == -1) {
      schemaId = lastAddedSchemaId;
      checkArgument(schemaId >= 0, "No previously added schema");
    }
    Optional<NessieSchema> schema = snapshot.schemaByIcebergId(schemaId);
    checkArgument(schema.isPresent(), "No schema with ID %s", schemaId);
    builder.currentSchemaId(schema.get().id());
  }

  public static void addPartitionSpec(AddPartitionSpec u, IcebergTableMetadataUpdateState state) {
    IcebergPartitionSpec spec = u.spec();
    checkArgument(spec.specId() >= -1, "Invalid spec-ID %s", spec.specId());
    NessieTableSnapshot snapshot = state.snapshot();

    // TODO re-check this ID-re-assignment block
    // assign fresh IDs (as Iceberg does), see org.apache.iceberg.TableMetadata.freshSpec
    IcebergPartitionSpec.Builder newSpecBuilder =
        IcebergPartitionSpec.builder().from(spec).specId(spec.specId()).fields(emptyList());
    int lastNewFieldId = Optional.ofNullable(snapshot.icebergLastPartitionId()).orElse(0);
    for (IcebergPartitionField field : spec.fields()) {
      int fieldId = field.fieldId();
      int sourceId = field.sourceId();
      lastNewFieldId = Math.max(lastNewFieldId, fieldId);
      sourceId = state.mapFieldId(sourceId, snapshot.currentSchemaId());
      newSpecBuilder.addField(
          IcebergPartitionField.builder().from(field).fieldId(fieldId).sourceId(sourceId).build());
    }
    spec = newSpecBuilder.build();

    // TODO also add the following checks from
    //  org.apache.iceberg.TableMetadata.Builder.addPartitionSpecInternal():
    //
    //    Schema schema = schemasById.get(currentSchemaId);
    //    PartitionSpec.checkCompatibility(spec, schema);
    //    ValidationException.check(
    //      formatVersion > 1 || PartitionSpec.hasSequentialIds(spec),
    //      "Spec does not use sequential IDs that are required in v1: %s",
    //      spec);

    // Check if a similar spec exists, and if so then update the "last added spec ID"
    // accordingly for a following "set-current-spec -1".
    ReuseOrCreate<IcebergPartitionSpec> reuseOrCreate = reuseOrCreateNewSpecId(spec, snapshot);
    spec = spec.withSpecId(reuseOrCreate.id());
    if (reuseOrCreate.value().isPresent()) {
      state.specAdded(spec.specId());
      return;
    }

    checkArgument(
        !snapshot.partitionDefinitionByIcebergId(spec.specId()).isPresent(),
        "A partition spec with ID %s already exists",
        spec.specId());

    Map<Integer, NessiePartitionField> icebergPartitionFields = new HashMap<>();
    for (NessiePartitionDefinition partitionDefinition : snapshot.partitionDefinitions()) {
      collectPartitionFields(partitionDefinition, icebergPartitionFields);
    }

    Map<Integer, NessieField> icebergFields = collectFieldsByIcebergId(snapshot.schemas());
    NessiePartitionDefinition def =
        icebergPartitionSpecToNessie(spec, icebergPartitionFields, icebergFields);
    checkArgument(
        def.icebergId() >= 0,
        "No Iceberg-ID for NessiePartitionDefinition, retrieved via spec-ID %s",
        u.spec().specId());
    state.builder().addPartitionDefinition(def).icebergLastPartitionId(lastNewFieldId);
    state.specAdded(spec.specId());
  }

  public static void removePartitionSpecs(
      IcebergMetadataUpdate.RemovePartitionSpecs removePartitionSpecs,
      IcebergTableMetadataUpdateState state) {
    NessieTableSnapshot snapshot = state.snapshot();

    var removeIds = new HashSet<>(removePartitionSpecs.specIds());
    var partitionDefs = new ArrayList<NessiePartitionDefinition>();

    for (NessiePartitionDefinition partitionDefinition : snapshot.partitionDefinitions()) {
      var icebergId = partitionDefinition.icebergId();
      if (!removeIds.contains(icebergId)) {
        partitionDefs.add(partitionDefinition);
      }
    }

    state.builder().partitionDefinitions(partitionDefs);
  }

  private static ReuseOrCreate<IcebergPartitionSpec> reuseOrCreateNewSpecId(
      IcebergPartitionSpec newSpec, NessieTableSnapshot snapshot) {
    var allFields = new HashMap<UUID, NessieField>();
    snapshot.schemas().forEach(schema -> collectFieldsByNessieId(schema, allFields));
    Iterator<IcebergPartitionSpec> specs =
        snapshot.partitionDefinitions().stream()
            .map(
                partDef ->
                    NessieModelIceberg.nessiePartitionDefinitionToIceberg(partDef, allFields::get))
            .iterator();

    // if the spec already exists, use the same ID. otherwise, use 1 more than the highest ID.
    int newSpecId = INITIAL_SPEC_ID;
    while (specs.hasNext()) {
      IcebergPartitionSpec spec = specs.next();
      if (newSpec.compatibleWith(spec)) {
        return reuseOrCreate(spec.specId(), spec);
      } else if (newSpecId <= spec.specId()) {
        newSpecId = spec.specId() + 1;
      }
    }

    return reuseOrCreate(newSpecId);
  }

  public static void setDefaultPartitionSpec(
      IcebergMetadataUpdate.SetDefaultPartitionSpec setDefaultPartitionSpec,
      IcebergTableMetadataUpdateState state) {
    NessieTableSnapshot snapshot = state.snapshot();
    int specId = setDefaultPartitionSpec.specId();
    if (specId == -1) {
      specId = state.lastAddedSpecId();
      checkArgument(specId >= 0, "No previously added partition spec");
    }
    Optional<NessiePartitionDefinition> partDef = snapshot.partitionDefinitionByIcebergId(specId);
    checkArgument(partDef.isPresent(), "No partition-spec with ID %s", specId);
    state.builder().currentPartitionDefinitionId(partDef.get().id());
  }

  public static void addSortOrder(AddSortOrder u, IcebergTableMetadataUpdateState state) {
    IcebergSortOrder sortOrder = u.sortOrder();
    checkArgument(sortOrder.orderId() >= -1, "Invalid order-ID %s", u.sortOrder().orderId());
    NessieTableSnapshot snapshot = state.snapshot();

    // TODO re-check this ID-re-assignment block
    // assign fresh IDs (as Iceberg does), see org.apache.iceberg.TableMetadata.freshSpec
    int orderId = sortOrder.orderId(); // TODO update this value
    IcebergSortOrder.Builder sortOrderBuilder =
        IcebergSortOrder.builder().from(sortOrder).orderId(orderId).fields(emptyList());
    for (IcebergSortField field : sortOrder.fields()) {
      int sourceId = field.sourceId();
      sourceId = state.mapFieldId(sourceId, snapshot.currentSchemaId());
      IcebergSortField newField = IcebergSortField.builder().from(field).sourceId(sourceId).build();
      sortOrderBuilder.addField(newField);
    }
    sortOrder = sortOrderBuilder.build();

    // Check if a similar order exists, and if so then update the "last added order ID"
    // accordingly for a following "set-current-order -1".
    ReuseOrCreate<IcebergSortOrder> reuseOrCreate =
        reuseOrCreateNewSortOrderId(sortOrder, snapshot);
    sortOrder = sortOrder.withOrderId(reuseOrCreate.id());
    if (reuseOrCreate.value().isPresent()) {
      state.sortOrderAdded(sortOrder.orderId());
      return;
    }

    checkArgument(
        !snapshot.sortDefinitionByIcebergId(sortOrder.orderId()).isPresent(),
        "A sort order with ID %s already exists",
        sortOrder.orderId());

    Map<Integer, NessieField> icebergFields = collectFieldsByIcebergId(snapshot.schemas());
    NessieSortDefinition sortDefinition = icebergSortOrderToNessie(sortOrder, icebergFields);
    checkArgument(
        sortDefinition.icebergSortOrderId() >= 0,
        "No Iceberg-ID for NessiePartitionDefinition, retrieved via order-ID %s",
        u.sortOrder().orderId());

    state.builder().addSortDefinition(sortDefinition);
    if (snapshot.currentSortDefinitionId() == null) {
      state.builder().currentSortDefinitionId(sortDefinition.id());
    }

    state.sortOrderAdded(sortOrder.orderId());
  }

  private static ReuseOrCreate<IcebergSortOrder> reuseOrCreateNewSortOrderId(
      IcebergSortOrder newOrder, NessieTableSnapshot snapshot) {
    var allFields = new HashMap<UUID, NessieField>();
    snapshot.schemas().forEach(schema -> collectFieldsByNessieId(schema, allFields));
    Iterator<IcebergSortOrder> orders =
        snapshot.sortDefinitions().stream()
            .map(
                sortDef ->
                    NessieModelIceberg.nessieSortDefinitionToIceberg(sortDef, allFields::get))
            .iterator();

    if (newOrder.isUnsorted()) {
      while (orders.hasNext()) {
        if (orders.next().isUnsorted()) {
          return reuseOrCreate(IcebergSortOrder.unsorted().orderId(), IcebergSortOrder.unsorted());
        }
      }
      return reuseOrCreate(IcebergSortOrder.unsorted().orderId());
    }

    // determine the next order id
    int newOrderId = INITIAL_SORT_ORDER_ID;
    while (orders.hasNext()) {
      IcebergSortOrder order = orders.next();
      if (order.sameOrder(newOrder)) {
        return reuseOrCreate(order.orderId(), order);
      } else if (newOrderId <= order.orderId()) {
        newOrderId = order.orderId() + 1;
      }
    }

    if (newOrderId == 0) {
      if (!newOrder.isUnsorted()) {
        newOrderId = 1;
      }
    } else if (newOrder.isUnsorted()) {
      newOrderId = 0;
    }

    return reuseOrCreate(newOrderId);
  }

  public static void setDefaultSortOrder(
      IcebergMetadataUpdate.SetDefaultSortOrder setDefaultSortOrder,
      IcebergTableMetadataUpdateState state) {
    NessieTableSnapshot snapshot = state.snapshot();

    int orderId = setDefaultSortOrder.sortOrderId();
    if (orderId == -1) {
      orderId = state.lastAddedOrderId();
      checkArgument(orderId >= 0, "No previously added sort-order");
    }

    Optional<NessieSortDefinition> sortDef = snapshot.sortDefinitionByIcebergId(orderId);
    checkArgument(sortDef.isPresent(), "No sort-order with ID %s", orderId);
    state.builder().currentSortDefinitionId(sortDef.get().id());
  }

  public static void addSnapshot(AddSnapshot u, IcebergTableMetadataUpdateState state) {
    IcebergSnapshot icebergSnapshot = u.snapshot();
    Integer schemaId = icebergSnapshot.schemaId();
    NessieTableSnapshot snapshot = state.snapshot();
    if (schemaId != null) {
      Optional<NessieSchema> schema = snapshot.schemaByIcebergId(schemaId);
      schema.ifPresent(s -> state.builder().currentSchemaId(s.id()));
    }

    state
        .builder()
        .icebergSnapshotId(icebergSnapshot.snapshotId())
        .icebergSnapshotSequenceNumber(icebergSnapshot.sequenceNumber())
        .icebergLastSequenceNumber(
            Math.max(
                safeUnbox(
                    snapshot.icebergLastSequenceNumber(),
                    IcebergTableMetadata.INITIAL_SEQUENCE_NUMBER),
                safeUnbox(
                    icebergSnapshot.sequenceNumber(),
                    IcebergTableMetadata.INITIAL_SEQUENCE_NUMBER)))
        .snapshotCreatedTimestamp(Instant.ofEpochMilli(icebergSnapshot.timestampMs()))
        .icebergSnapshotSummary(icebergSnapshot.summary())
        .icebergManifestListLocation(icebergSnapshot.manifestList())
        .icebergManifestFileLocations(icebergSnapshot.manifests());

    state.snapshotAdded(icebergSnapshot);
  }

  public static void addViewVersion(
      IcebergMetadataUpdate.AddViewVersion addViewVersion, IcebergViewMetadataUpdateState state) {
    // TODO re-check this
    IcebergViewVersion viewVersion = addViewVersion.viewVersion();

    NessieViewSnapshot snapshot = state.snapshot();

    if (viewVersion.schemaId() == -1) {
      viewVersion = viewVersion.withSchemaId(state.lastAddedSchemaId());
    }
    Long currentVersionId = snapshot.icebergCurrentVersionId();
    if (currentVersionId != null && viewVersion.versionId() <= currentVersionId) {
      viewVersion = viewVersion.withVersionId(currentVersionId + 1);
    }

    Instant now = Instant.ofEpochMilli(viewVersion.timestampMs());
    state
        .builder()
        .icebergCurrentVersionId(viewVersion.versionId())
        .icebergDefaultCatalog(viewVersion.defaultCatalog())
        .icebergVersionSummary(viewVersion.summary())
        .icebergNamespace(viewVersion.defaultNamespace().levels())
        .representations(
            viewVersion.representations().stream()
                .map(NessieModelIceberg::icebergViewRepresentationToNessie)
                .collect(Collectors.toList()))
        .snapshotCreatedTimestamp(now)
        .lastUpdatedTimestamp(now);

    snapshot
        .schemaByIcebergId(viewVersion.schemaId())
        .ifPresent(schema -> state.builder().currentSchemaId(schema.id()));

    state.versionAdded(viewVersion.versionId());
  }

  public static void setCurrentViewVersion(
      IcebergMetadataUpdate.SetCurrentViewVersion setCurrentViewVersion,
      IcebergViewMetadataUpdateState state) {
    long versionId = setCurrentViewVersion.viewVersionId();
    if (versionId == -1) {
      versionId = state.lastAddedVersionId();
      checkArgument(versionId >= 0, "No previously added view version");
    }
    // TODO re-check this
    state.builder().icebergCurrentVersionId(versionId);
  }

  public static Content icebergMetadataToContent(
      String metadataJsonLocation, IcebergTableMetadata snapshot, String contentId) {
    return IcebergTable.of(
        metadataJsonLocation,
        snapshot.currentSnapshotIdAsLong(),
        safeUnbox(snapshot.currentSchemaId(), INITIAL_SCHEMA_ID),
        safeUnbox(snapshot.defaultSpecId(), INITIAL_SPEC_ID),
        safeUnbox(snapshot.defaultSortOrderId(), INITIAL_SORT_ORDER_ID),
        contentId);
  }

  public static Content icebergMetadataToContent(
      String location, IcebergViewMetadata snapshot, String contentId) {
    IcebergViewVersion version = snapshot.currentVersion();
    return IcebergView.of(contentId, location, snapshot.currentVersionId(), version.schemaId());
  }

  public static String typeToEntityName(Content.Type type) {
    if (ICEBERG_TABLE.equals(type)) {
      return "Table";
    }
    if (ICEBERG_VIEW.equals(type)) {
      return "View";
    }
    if (NAMESPACE.equals(type)) {
      return "Namespace";
    }
    return type.name();
  }
}

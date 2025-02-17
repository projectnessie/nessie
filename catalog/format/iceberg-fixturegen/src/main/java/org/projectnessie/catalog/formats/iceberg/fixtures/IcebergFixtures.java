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
package org.projectnessie.catalog.formats.iceberg.fixtures;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.INITIAL_SPEC_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.INITIAL_COLUMN_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.INITIAL_SCHEMA_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.INITIAL_SORT_ORDER_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.INITIAL_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.INITIAL_SEQUENCE_NUMBER;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.NO_SNAPSHOT_ID;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.binaryType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.booleanType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.dateType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.decimalType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.doubleType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.fixedType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.floatType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.integerType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.listType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.longType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.mapType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.structType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timeType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestampType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestamptzType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.uuidType;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.stream.Stream;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergBlobMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotLogEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotRef;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergStatisticsFile;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewHistoryEntry;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewVersion;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;

public class IcebergFixtures {
  public static Stream<IcebergType> icebergTypes() {
    AtomicInteger idGen = new AtomicInteger();
    return icebergTypes(idGen::incrementAndGet);
  }

  public static Stream<IcebergType> icebergTypes(IntSupplier idSupplier) {
    return Stream.of(
        booleanType(),
        uuidType(),
        stringType(),
        binaryType(),
        integerType(),
        longType(),
        floatType(),
        doubleType(),
        dateType(),
        timeType(),
        structType(
            singletonList(nestedField(idSupplier.getAsInt(), "field11", true, stringType(), null)),
            null),
        structType(
            singletonList(nestedField(idSupplier.getAsInt(), "field11", false, stringType(), null)),
            null),
        listType(idSupplier.getAsInt(), stringType(), true),
        listType(idSupplier.getAsInt(), stringType(), false),
        listType(idSupplier.getAsInt(), uuidType(), true),
        listType(idSupplier.getAsInt(), uuidType(), false),
        mapType(idSupplier.getAsInt(), stringType(), idSupplier.getAsInt(), dateType(), true),
        mapType(idSupplier.getAsInt(), stringType(), idSupplier.getAsInt(), dateType(), false),
        mapType(idSupplier.getAsInt(), uuidType(), idSupplier.getAsInt(), timeType(), true),
        mapType(idSupplier.getAsInt(), uuidType(), idSupplier.getAsInt(), timeType(), false),
        decimalType(10, 3),
        fixedType(42),
        timestampType(),
        timestamptzType());
  }

  public static IcebergSchema icebergSchemaAllTypes() {
    IcebergSchema.Builder icebergSchemaBuilder =
        IcebergSchema.builder().schemaId(42).type("struct");

    int i = 0;
    for (Iterator<IcebergType> iter = icebergTypes().iterator(); iter.hasNext(); i++) {
      IcebergType icebergType = iter.next();
      icebergSchemaBuilder.addFields(
          IcebergNestedField.builder()
              .id(1001 + i)
              .doc("doc_" + i)
              .type(icebergType)
              .name("field_name_" + i)
              .required((i & 1) == 0)
              .build());
    }

    return icebergSchemaBuilder.build();
  }

  public static IcebergTableMetadata.Builder tableMetadataBare() {
    return IcebergTableMetadata.builder()
        .tableUuid(UUID.randomUUID().toString())
        .lastUpdatedMs(111111111L)
        .location("table-location")
        .currentSnapshotId(NO_SNAPSHOT_ID)
        .lastColumnId(INITIAL_COLUMN_ID)
        .lastPartitionId(INITIAL_PARTITION_ID)
        .lastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
        .currentSchemaId(INITIAL_SCHEMA_ID)
        .defaultSortOrderId(INITIAL_SORT_ORDER_ID)
        .defaultSpecId(INITIAL_SPEC_ID)
        .putProperty("prop", "value");
  }

  public static IcebergTableMetadata.Builder tableMetadataBareWithSchema() {
    IcebergSchema schemaAllTypes = icebergSchemaAllTypes();

    return IcebergTableMetadata.builder()
        .tableUuid(UUID.randomUUID().toString())
        .lastUpdatedMs(111111111L)
        .location("table-location")
        .currentSnapshotId(NO_SNAPSHOT_ID)
        .lastColumnId(schemaAllTypes.fields().get(schemaAllTypes.fields().size() - 1).id())
        .lastPartitionId(INITIAL_PARTITION_ID)
        .lastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
        .currentSchemaId(schemaAllTypes.schemaId())
        .defaultSortOrderId(INITIAL_SORT_ORDER_ID)
        .defaultSpecId(INITIAL_SPEC_ID)
        .putProperty("prop", "value")
        .addSchemas(schemaAllTypes);
  }

  public static IcebergTableMetadata.Builder tableMetadataSimple() {
    IcebergSchema schemaAllTypes = icebergSchemaAllTypes();

    return IcebergTableMetadata.builder()
        .tableUuid(UUID.randomUUID().toString())
        .lastUpdatedMs(111111111L)
        .location("table-location")
        .currentSnapshotId(11L)
        .lastColumnId(schemaAllTypes.fields().get(schemaAllTypes.fields().size() - 1).id())
        .lastPartitionId(INITIAL_PARTITION_ID)
        .lastSequenceNumber(INITIAL_SEQUENCE_NUMBER)
        .currentSchemaId(schemaAllTypes.schemaId())
        .defaultSortOrderId(INITIAL_SORT_ORDER_ID)
        .defaultSpecId(INITIAL_SPEC_ID)
        .putProperty("prop", "value")
        .addSchemas(schemaAllTypes)
        .addSnapshots(
            IcebergSnapshot.builder()
                .snapshotId(11)
                .schemaId(schemaAllTypes.schemaId())
                .putSummary("operation", "testing")
                .sequenceNumber(123L)
                .timestampMs(12345678L)
                .build())
        .putRef("main", IcebergSnapshotRef.builder().type("branch").snapshotId(11).build())
        .addSnapshotLog(
            IcebergSnapshotLogEntry.builder().snapshotId(11).timestampMs(12345678L).build());
  }

  public static IcebergViewMetadata.Builder viewMetadataSimple() {
    IcebergSchema schemaAllTypes = icebergSchemaAllTypes();

    return IcebergViewMetadata.builder()
        .viewUuid(UUID.randomUUID().toString())
        .location("view-location")
        .currentVersionId(11)
        .putProperty("prop", "value")
        .addSchemas(schemaAllTypes)
        .addVersions(
            IcebergViewVersion.builder()
                .versionId(11)
                .schemaId(schemaAllTypes.schemaId())
                .putSummary("operation", "testing")
                .timestampMs(12345678L)
                .defaultNamespace(IcebergNamespace.EMPTY_ICEBERG_NAMESPACE)
                .build())
        .addVersionLog(IcebergViewHistoryEntry.icebergViewHistoryEntry(12345678L, 11));
  }

  public static IcebergTableMetadata.Builder tableMetadataWithStatistics() {
    IcebergStatisticsFile statisticsFile =
        IcebergStatisticsFile.statisticsFile(
            11,
            "statistics-path",
            123456L,
            123L,
            singletonList(
                IcebergBlobMetadata.blobMetadata("type", 11, 123, singletonList(1), emptyMap())));
    IcebergPartitionStatisticsFile partitionStatistic =
        IcebergPartitionStatisticsFile.partitionStatisticsFile(11, "statistics-path", 123456L);
    return tableMetadataSimple()
        .addStatistics(statisticsFile)
        .addPartitionStatistic(partitionStatistic);
  }
}

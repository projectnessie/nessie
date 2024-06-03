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

import static java.util.Collections.singletonList;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.icebergSchemaAllTypes;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataBare;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataBareWithSchema;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataSimple;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataWithStatistics;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.MIN_PARTITION_ID;
import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTransform;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewVersion;
import org.projectnessie.catalog.formats.iceberg.types.IcebergType;
import org.projectnessie.catalog.model.NessieTable;
import org.projectnessie.catalog.model.NessieView;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieFieldTransform;
import org.projectnessie.catalog.model.schema.NessieNullOrder;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.schema.NessieSortDirection;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieModelIceberg {
  @InjectSoftAssertions protected SoftAssertions soft;

  static <T> T icebergJsonSerializeDeserialize(T obj, Class<T> type) throws IOException {
    ObjectMapper mapper = IcebergJson.objectMapper();
    String json = mapper.writeValueAsString(obj);
    return mapper.readValue(json, type);
  }

  @ParameterizedTest
  @MethodSource
  public void transformSpec(IcebergTransform iceberg, NessieFieldTransform nessie) {
    NessieFieldTransform toNessie = NessieModelIceberg.icebergTransformToNessie(iceberg);
    soft.assertThat(toNessie).isEqualTo(nessie);

    IcebergTransform toIceberg = NessieModelIceberg.nessieTransformToIceberg(nessie);
    soft.assertThat(toIceberg).isEqualTo(iceberg);
  }

  static Stream<Arguments> transformSpec() {
    return Stream.of(
        arguments(IcebergTransform.identity(), NessieFieldTransform.identity()),
        arguments(IcebergTransform.voidTransform(), NessieFieldTransform.voidTransform()),
        arguments(IcebergTransform.bucket(1), NessieFieldTransform.bucket(1)),
        arguments(IcebergTransform.bucket(42), NessieFieldTransform.bucket(42)),
        arguments(
            IcebergTransform.unknownTransform("unknown_transform"),
            NessieFieldTransform.unknownTransform("unknown_transform")),
        arguments(IcebergTransform.hour(), NessieFieldTransform.hour()),
        arguments(IcebergTransform.day(), NessieFieldTransform.day()),
        arguments(IcebergTransform.month(), NessieFieldTransform.month()),
        arguments(IcebergTransform.year(), NessieFieldTransform.year()),
        arguments(IcebergTransform.truncate(42), NessieFieldTransform.truncate(42)),
        arguments(IcebergTransform.truncate(1), NessieFieldTransform.truncate(1)));
  }

  @ParameterizedTest
  @MethodSource("icebergTypes")
  public void icebergNessieSchema(IcebergType icebergType) throws Exception {
    IcebergSchema icebergSchema =
        IcebergSchema.builder()
            .schemaId(42)
            .type("struct")
            .addFields(
                IcebergNestedField.builder()
                    .id(123)
                    .doc("doc")
                    .type(icebergType)
                    .name("field_name")
                    .required(true)
                    .build())
            .build();
    soft.assertThat(icebergJsonSerializeDeserialize(icebergSchema, IcebergSchema.class))
        .isEqualTo(icebergSchema);

    Map<Integer, NessieField> fieldsMap = new HashMap<>();
    Map<Integer, NessiePartitionField> partitionFieldsMap = new HashMap<>();
    NessieSchema nessieSchema =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergSchema, fieldsMap);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieSchema, NessieSchema.class))
        .isEqualTo(nessieSchema);

    IcebergSchema icebergAgain = NessieModelIceberg.nessieSchemaToIcebergSchema(nessieSchema);

    soft.assertThat(icebergAgain).isEqualTo(icebergSchema);

    NessieSchema nessieAgain =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergAgain, fieldsMap);

    soft.assertThat(nessieAgain).isEqualTo(nessieSchema);

    // partition-spec

    IcebergPartitionSpec icebergPartitionSpec =
        IcebergPartitionSpec.partitionSpec(
            42,
            singletonList(
                partitionField(
                    "field_part", IcebergTransform.identity().toString(), 123, MIN_PARTITION_ID)));
    soft.assertThat(
            icebergJsonSerializeDeserialize(icebergPartitionSpec, IcebergPartitionSpec.class))
        .isEqualTo(icebergPartitionSpec);

    NessiePartitionDefinition nessiePartitionDefinition =
        NessieModelIceberg.icebergPartitionSpecToNessie(
            icebergPartitionSpec, partitionFieldsMap, fieldsMap);
    soft.assertThat(
            icebergJsonSerializeDeserialize(
                nessiePartitionDefinition, NessiePartitionDefinition.class))
        .isEqualTo(nessiePartitionDefinition);

    IcebergPartitionSpec icebergPartitionSpecConv =
        NessieModelIceberg.nessiePartitionDefinitionToIceberg(
            nessiePartitionDefinition, nessieSchema.fieldsById()::get);
    soft.assertThat(icebergPartitionSpecConv).isEqualTo(icebergPartitionSpec);

    NessiePartitionDefinition nessiePartitionDefinitionAgain =
        NessieModelIceberg.icebergPartitionSpecToNessie(
            icebergPartitionSpecConv, partitionFieldsMap, fieldsMap);
    soft.assertThat(nessiePartitionDefinitionAgain).isEqualTo(nessiePartitionDefinition);

    // sort-order

    IcebergSortOrder icebergSortOrder =
        IcebergSortOrder.sortOrder(
            42,
            singletonList(
                IcebergSortField.sortField(
                    IcebergTransform.identity().toString(),
                    123,
                    NessieSortDirection.ASC,
                    NessieNullOrder.NULLS_FIRST)));
    soft.assertThat(icebergJsonSerializeDeserialize(icebergSortOrder, IcebergSortOrder.class))
        .isEqualTo(icebergSortOrder);

    NessieSortDefinition nessieSortDefinition =
        NessieModelIceberg.icebergSortOrderToNessie(icebergSortOrder, fieldsMap);
    soft.assertThat(
            icebergJsonSerializeDeserialize(nessieSortDefinition, NessieSortDefinition.class))
        .isEqualTo(nessieSortDefinition);

    IcebergSortOrder icebergSortOrderConv =
        NessieModelIceberg.nessieSortDefinitionToIceberg(
            nessieSortDefinition, nessieSchema.fieldsById()::get);
    soft.assertThat(icebergSortOrderConv).isEqualTo(icebergSortOrder);

    NessieSortDefinition nessieSortDefinitionAgain =
        NessieModelIceberg.icebergSortOrderToNessie(icebergSortOrderConv, fieldsMap);
    soft.assertThat(nessieSortDefinitionAgain).isEqualTo(nessieSortDefinition);
  }

  @ParameterizedTest
  @MethodSource
  public void icebergTableMetadata(IcebergTableMetadata icebergTableMetadata) throws Exception {
    soft.assertThat(
            icebergJsonSerializeDeserialize(icebergTableMetadata, IcebergTableMetadata.class))
        .isEqualTo(icebergTableMetadata);

    NessieTable table =
        NessieTable.builder()
            .createdTimestamp(Instant.now())
            .icebergUuid(icebergTableMetadata.tableUuid())
            .nessieContentId(UUID.randomUUID().toString())
            .tableFormat(TableFormat.ICEBERG)
            .build();

    NessieId snapshotId = NessieId.randomNessieId();

    NessieTableSnapshot nessie =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, null, table, icebergTableMetadata, IcebergSnapshot::manifestList);
    soft.assertThat(icebergJsonSerializeDeserialize(nessie, NessieTableSnapshot.class))
        .isEqualTo(nessie);

    IcebergTableMetadata iceberg =
        NessieModelIceberg.nessieTableSnapshotToIceberg(nessie, Optional.empty(), properties -> {});
    IcebergTableMetadata icebergWithCatalogProps =
        IcebergTableMetadata.builder()
            .from(icebergTableMetadata)
            .putAllProperties(
                iceberg.properties().entrySet().stream()
                    .filter(e -> e.getKey().startsWith("nessie."))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
            .schema(
                icebergTableMetadata.formatVersion() > 1
                    ? null
                    : iceberg.schemas().isEmpty() ? null : iceberg.schemas().get(0))
            .build();
    soft.assertThat(iceberg).isEqualTo(icebergWithCatalogProps);

    NessieTableSnapshot nessieAgain =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, nessie, nessie.entity(), iceberg, IcebergSnapshot::manifestList);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieAgain, NessieTableSnapshot.class))
        .isEqualTo(nessieAgain);
  }

  static Stream<IcebergTableMetadata> icebergTableMetadata() {
    return Stream.of(
            // bare one
            tableMetadataBare(),
            // just a schema
            tableMetadataBareWithSchema(),
            // snapshot
            tableMetadataSimple(),
            // statistics
            tableMetadataWithStatistics())
        .flatMap(
            builder ->
                Stream.of(
                    builder.formatVersion(IcebergSpec.V1.version()).build(),
                    builder.formatVersion(IcebergSpec.V2.version()).build()));
  }

  @Test
  public void icebergNessieSchemaAllTypes() throws Exception {
    IcebergSchema icebergSchema = icebergSchemaAllTypes();
    soft.assertThat(icebergJsonSerializeDeserialize(icebergSchema, IcebergSchema.class))
        .isEqualTo(icebergSchema);

    Map<Integer, NessieField> fieldsMap = new HashMap<>();
    NessieSchema nessieSchema =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergSchema, fieldsMap);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieSchema, NessieSchema.class))
        .isEqualTo(nessieSchema);

    IcebergSchema icebergAgain = NessieModelIceberg.nessieSchemaToIcebergSchema(nessieSchema);

    soft.assertThat(icebergAgain).isEqualTo(icebergSchema);

    NessieSchema nessieAgain =
        NessieModelIceberg.icebergSchemaToNessieSchema(icebergAgain, fieldsMap);

    soft.assertThat(nessieAgain).isEqualTo(nessieSchema);
  }

  @ParameterizedTest
  @MethodSource("icebergTypes")
  public void icebergNessieTypeConversion(IcebergType icebergType) throws Exception {
    soft.assertThat(icebergJsonSerializeDeserialize(icebergType, IcebergType.class))
        .isEqualTo(icebergType);

    Map<Integer, NessieField> icebergFieldIdToField = new HashMap<>();

    NessieTypeSpec nessieType =
        NessieModelIceberg.icebergTypeToNessieType(icebergType, icebergFieldIdToField);
    soft.assertThat(icebergJsonSerializeDeserialize(nessieType, NessieTypeSpec.class))
        .isEqualTo(nessieType);

    IcebergType icebergAgain = NessieModelIceberg.nessieTypeToIcebergType(nessieType);
    soft.assertThat(icebergAgain).isEqualTo(icebergType);
    NessieTypeSpec nessieAgain =
        NessieModelIceberg.icebergTypeToNessieType(icebergAgain, icebergFieldIdToField);
    soft.assertThat(nessieAgain).isEqualTo(nessieType);

    // Verify that generated 'NessieId's are deterministic
    soft.assertThat(nessieIdHasher("NessieTypeSpec").hash(nessieType).generate())
        .isEqualTo(nessieIdHasher("NessieTypeSpec").hash(nessieType).generate())
        .isEqualTo(nessieIdHasher("NessieTypeSpec").hash(nessieAgain).generate());
  }

  static Stream<IcebergType> icebergTypes() {
    return IcebergFixtures.icebergTypes();
  }

  @Test
  public void collectTableLocations() {
    String uuid = UUID.randomUUID().toString();
    String location1 = "s3://mybucket/tables/my/table";
    String location2 = "s3://otherbucket/tables/my/table";
    String location3 = "s3://otherbucket/betttertables/my/table";

    IcebergTableMetadata meta1 =
        IcebergTableMetadata.builder()
            .location(location1)
            .tableUuid(uuid)
            .formatVersion(2)
            .lastUpdatedMs(1L)
            .lastColumnId(0)
            .currentSnapshotId(-1L)
            .build();
    IcebergTableMetadata meta2 =
        IcebergTableMetadata.builder().from(meta1).location(location1).build();
    IcebergTableMetadata meta3 =
        IcebergTableMetadata.builder().from(meta1).location(location2).build();
    IcebergTableMetadata meta4 =
        IcebergTableMetadata.builder().from(meta1).location(location3).build();

    NessieId snapshotId = NessieId.randomNessieId();
    NessieTable table =
        NessieTable.builder()
            .tableFormat(TableFormat.ICEBERG)
            .nessieContentId(uuid)
            .icebergUuid(uuid)
            .createdTimestamp(Instant.EPOCH)
            .build();

    NessieTableSnapshot snap1 =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, null, table, meta1, IcebergSnapshot::manifestList);
    NessieTableSnapshot snap2 =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, snap1, table, meta2, IcebergSnapshot::manifestList);
    NessieTableSnapshot snap3 =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, snap2, table, meta3, IcebergSnapshot::manifestList);
    NessieTableSnapshot snap4 =
        NessieModelIceberg.icebergTableSnapshotToNessie(
            snapshotId, snap3, table, meta4, IcebergSnapshot::manifestList);

    soft.assertThat(snap1.icebergLocation()).isEqualTo(location1);
    soft.assertThat(snap1.additionalKnownLocations()).isEmpty();
    soft.assertThat(snap2.icebergLocation()).isEqualTo(location1);
    soft.assertThat(snap2.additionalKnownLocations()).isEmpty();
    soft.assertThat(snap3.icebergLocation()).isEqualTo(location2);
    soft.assertThat(snap3.additionalKnownLocations()).containsExactlyInAnyOrder(location1);
    soft.assertThat(snap4.icebergLocation()).isEqualTo(location3);
    soft.assertThat(snap4.additionalKnownLocations())
        .containsExactlyInAnyOrder(location1, location2);
  }

  @Test
  public void collectViewLocations() {
    String uuid = UUID.randomUUID().toString();
    String location1 = "s3://mybucket/tables/my/table";
    String location2 = "s3://otherbucket/tables/my/table";
    String location3 = "s3://otherbucket/betttertables/my/table";

    IcebergViewVersion version =
        IcebergViewVersion.builder()
            .versionId(1)
            .timestampMs(1L)
            .schemaId(1)
            .defaultNamespace(IcebergNamespace.EMPTY_ICEBERG_NAMESPACE)
            .build();
    IcebergViewMetadata meta1 =
        IcebergViewMetadata.builder()
            .location(location1)
            .viewUuid(uuid)
            .formatVersion(2)
            .currentVersionId(1)
            .addVersions(version)
            .build();
    IcebergViewMetadata meta2 =
        IcebergViewMetadata.builder().from(meta1).location(location1).build();
    IcebergViewMetadata meta3 =
        IcebergViewMetadata.builder().from(meta1).location(location2).build();
    IcebergViewMetadata meta4 =
        IcebergViewMetadata.builder().from(meta1).location(location3).build();

    NessieId snapshotId = NessieId.randomNessieId();
    NessieView view =
        NessieView.builder()
            .tableFormat(TableFormat.ICEBERG)
            .nessieContentId(uuid)
            .icebergUuid(uuid)
            .createdTimestamp(Instant.EPOCH)
            .build();

    NessieViewSnapshot snap1 =
        NessieModelIceberg.icebergViewSnapshotToNessie(snapshotId, null, view, meta1);
    NessieViewSnapshot snap2 =
        NessieModelIceberg.icebergViewSnapshotToNessie(snapshotId, snap1, view, meta2);
    NessieViewSnapshot snap3 =
        NessieModelIceberg.icebergViewSnapshotToNessie(snapshotId, snap2, view, meta3);
    NessieViewSnapshot snap4 =
        NessieModelIceberg.icebergViewSnapshotToNessie(snapshotId, snap3, view, meta4);

    soft.assertThat(snap1.icebergLocation()).isEqualTo(location1);
    soft.assertThat(snap1.additionalKnownLocations()).isEmpty();
    soft.assertThat(snap2.icebergLocation()).isEqualTo(location1);
    soft.assertThat(snap2.additionalKnownLocations()).isEmpty();
    soft.assertThat(snap3.icebergLocation()).isEqualTo(location2);
    soft.assertThat(snap3.additionalKnownLocations()).containsExactlyInAnyOrder(location1);
    soft.assertThat(snap4.icebergLocation()).isEqualTo(location3);
    soft.assertThat(snap4.additionalKnownLocations())
        .containsExactlyInAnyOrder(location1, location2);
  }
}

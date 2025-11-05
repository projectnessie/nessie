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
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.icebergSchemaAllTypes;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataBare;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataBareWithSchema;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataSimple;
import static org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures.tableMetadataWithStatistics;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.INITIAL_SPEC_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.MIN_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.INITIAL_SCHEMA_ID;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField.sortField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.INITIAL_SORT_ORDER_ID;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.collectFieldsByIcebergId;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergPartitionSpecToNessie;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergSchemaToNessieSchema;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.icebergSortOrderToNessie;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessiePartitionDefinitionToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.nessieSortDefinitionToIceberg;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergTableSnapshot;
import static org.projectnessie.catalog.formats.iceberg.nessie.NessieModelIceberg.newIcebergViewSnapshot;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddPartitionSpec.addPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSchema.addSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.AddSortOrder.addSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetCurrentSchema.setCurrentSchema;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultPartitionSpec.setDefaultPartitionSpec;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetDefaultSortOrder.setDefaultSortOrder;
import static org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate.SetLocation.setTrustedLocation;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.binaryType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.integerType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.listType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.longType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.mapType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.structType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.timestamptzType;
import static org.projectnessie.catalog.model.id.NessieIdHasher.nessieIdHasher;
import static org.projectnessie.catalog.model.schema.NessieNullOrder.NULLS_LAST;
import static org.projectnessie.catalog.model.schema.NessieSortDirection.ASC;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.formats.iceberg.fixtures.IcebergFixtures;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergJson;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
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
import org.projectnessie.catalog.model.schema.NessieStruct;
import org.projectnessie.catalog.model.schema.types.NessieType;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.model.ContentKey;

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

  @SuppressWarnings("deprecation") // NestedField.of removed in Iceberg 2.0
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void partitioningNestedStruct(boolean firstLevel) throws Exception {
    var headersKv =
        Types.StructType.of(
            Types.NestedField.of(7, true, "key", Types.BinaryType.get()),
            Types.NestedField.of(8, true, "value", Types.BinaryType.get()));

    var fieldTimestamp =
        Types.NestedField.of(4, false, "timestamp", Types.TimestampType.withoutZone());
    var fieldTimestampDeep =
        Types.NestedField.of(13, false, "deep_timestamp", Types.TimestampType.withZone());
    var deeplyNestedStruct =
        Types.NestedField.of(
            10,
            false,
            "deeply",
            Types.StructType.of(
                Types.NestedField.of(11, true, "deep_dec", Types.DecimalType.of(10, 3)),
                Types.NestedField.of(12, true, "deep_time", Types.TimeType.get()),
                fieldTimestampDeep,
                Types.NestedField.of(14, true, "deep_bin", Types.BinaryType.get()),
                Types.NestedField.of(
                    15,
                    true,
                    "deep_string",
                    Types.ListType.ofRequired(16, Types.StringType.get()))));
    var systemFields =
        Types.StructType.of(
            Types.NestedField.of(2, false, "partition", Types.IntegerType.get()),
            Types.NestedField.of(3, false, "offset", Types.LongType.get()),
            fieldTimestamp,
            Types.NestedField.of(5, false, "headers", Types.ListType.ofRequired(6, headersKv)),
            Types.NestedField.of(9, false, "key", Types.BinaryType.get()),
            deeplyNestedStruct);

    var fieldTestSchema = Types.NestedField.of(1, false, "test_schema", systemFields);
    var schema = new Schema(List.of(fieldTestSchema));

    var sourceName =
        fieldTestSchema.name()
            + '.'
            + (firstLevel
                ? fieldTimestamp.name()
                : (deeplyNestedStruct.name() + '.' + fieldTimestampDeep.name()));

    var spec =
        PartitionSpec.builderFor(schema).withSpecId(42).day(sourceName, "daytime_day").build();
    var sort = SortOrder.builderFor(schema).withOrderId(43).asc(sourceName).build();

    var schemaJson = SchemaParser.toJson(schema);
    var specJson = PartitionSpecParser.toJson(spec);
    var sortJson = SortOrderParser.toJson(sort);

    var nessieMapper = new ObjectMapper().findAndRegisterModules();
    var icebergSchema = nessieMapper.readValue(schemaJson, IcebergSchema.class);
    var icebergSpec = nessieMapper.readValue(specJson, IcebergPartitionSpec.class);
    var icebergSort = nessieMapper.readValue(sortJson, IcebergSortOrder.class);

    var icebergFields = new HashMap<Integer, NessieField>();
    var nessieSchema = icebergSchemaToNessieSchema(icebergSchema, icebergFields);

    var partitionFields1 = new HashMap<Integer, NessiePartitionField>();
    soft.assertThatCode(
            () -> icebergPartitionSpecToNessie(icebergSpec, partitionFields1, icebergFields))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> icebergSortOrderToNessie(icebergSort, icebergFields))
        .doesNotThrowAnyException();

    var collectedFields = collectFieldsByIcebergId(List.of(nessieSchema));
    // Remove the fields in the headersKv list (makes no sense to partition-by or sort-by fields in
    // collections)
    headersKv.fields().stream().map(Types.NestedField::fieldId).forEach(icebergFields::remove);
    soft.assertThat(collectedFields).containsExactlyInAnyOrderEntriesOf(icebergFields);

    var partitionFields2 = new HashMap<Integer, NessiePartitionField>();
    soft.assertThatCode(
            () -> icebergPartitionSpecToNessie(icebergSpec, partitionFields2, collectedFields))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> icebergSortOrderToNessie(icebergSort, collectedFields))
        .doesNotThrowAnyException();

    var allFields = new HashMap<UUID, NessieField>();
    NessieModelIceberg.collectFieldsByNessieId(nessieSchema, allFields);

    var nessiePartDef =
        icebergPartitionSpecToNessie(icebergSpec, partitionFields2, collectedFields);
    var nessieSort = icebergSortOrderToNessie(icebergSort, collectedFields);
    soft.assertThatCode(() -> nessiePartitionDefinitionToIceberg(nessiePartDef, allFields::get))
        .doesNotThrowAnyException();
    soft.assertThatCode(() -> nessieSortDefinitionToIceberg(nessieSort, allFields::get))
        .doesNotThrowAnyException();

    var toIcebergSpec = nessiePartitionDefinitionToIceberg(nessiePartDef, allFields::get);
    var toIcebergSort = nessieSortDefinitionToIceberg(nessieSort, allFields::get);
    soft.assertThat(toIcebergSpec).isEqualTo(icebergSpec);
    soft.assertThat(toIcebergSort).isEqualTo(icebergSort);

    var toIcebergSpecJson = nessieMapper.writeValueAsString(toIcebergSpec);
    var toIcebergSortJson = nessieMapper.writeValueAsString(toIcebergSort);

    var toSpec = PartitionSpecParser.fromJson(schema, toIcebergSpecJson);
    var toSort = SortOrderParser.fromJson(schema, toIcebergSortJson);

    soft.assertThat(toSpec).isEqualTo(spec);
    soft.assertThat(toSort).isEqualTo(sort);
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
                sortField(
                    IcebergTransform.identity().toString(),
                    123,
                    ASC,
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

  static Stream<Arguments> icebergNested() {
    return Stream.of(
        //
        // Simple case, just some columns
        arguments(
            IcebergSchema.builder()
                .schemaId(11)
                .addFields(
                    nestedField(100, "key", false, binaryType(), null),
                    nestedField(101, "value", false, binaryType(), null),
                    nestedField(102, "topic", false, stringType(), null),
                    nestedField(103, "partition", false, integerType(), null))
                .build(),
            IcebergSchema.builder()
                .schemaId(INITIAL_SCHEMA_ID)
                .addFields(
                    nestedField(1, "key", false, binaryType(), null),
                    nestedField(2, "value", false, binaryType(), null),
                    nestedField(3, "topic", false, stringType(), null),
                    nestedField(4, "partition", false, integerType(), null))
                .build(),
            4),
        //
        // Complex case, multiple columns, nested structures, last field has a complex type with the
        // highest ID via a field-ID in a non-nested-field type
        arguments(
            IcebergSchema.builder()
                .schemaId(42)
                .addFields(
                    nestedField(100, "key", false, binaryType(), null),
                    nestedField(101, "value", false, binaryType(), null),
                    nestedField(102, "topic", false, stringType(), null),
                    nestedField(103, "partition", false, integerType(), null),
                    nestedField(104, "offset", false, longType(), null),
                    nestedField(105, "timestamp", false, timestamptzType(), null),
                    nestedField(106, "timestampType", false, integerType(), null),
                    nestedField(
                        107,
                        "headers",
                        false,
                        listType(
                            108,
                            structType(
                                List.of(
                                    nestedField(109, "key", false, stringType(), null),
                                    nestedField(110, "value", false, stringType(), null)),
                                null),
                            false),
                        null),
                    nestedField(
                        111,
                        "fancy_properties",
                        false,
                        mapType(
                            112,
                            structType(
                                List.of(
                                    nestedField(
                                        113,
                                        "f1",
                                        false,
                                        listType(114, integerType(), false),
                                        null)),
                                null),
                            115,
                            structType(
                                List.of(
                                    nestedField(
                                        116,
                                        "struct",
                                        false,
                                        listType(
                                            117,
                                            structType(
                                                List.of(
                                                    nestedField(
                                                        118, "key", false, stringType(), null),
                                                    nestedField(
                                                        119, "value", false, stringType(), null)),
                                                null),
                                            false),
                                        null)),
                                null),
                            false),
                        null),
                    nestedField(
                        120,
                        "properties",
                        false,
                        mapType(121, stringType(), 122, stringType(), false),
                        null))
                .build(),
            IcebergSchema.builder()
                .schemaId(INITIAL_SCHEMA_ID)
                .addFields(
                    nestedField(1, "key", false, binaryType(), null),
                    nestedField(2, "value", false, binaryType(), null),
                    nestedField(3, "topic", false, stringType(), null),
                    nestedField(4, "partition", false, integerType(), null),
                    nestedField(5, "offset", false, longType(), null),
                    nestedField(6, "timestamp", false, timestamptzType(), null),
                    nestedField(7, "timestampType", false, integerType(), null),
                    nestedField(
                        8,
                        "headers",
                        false,
                        listType(
                            9,
                            structType(
                                List.of(
                                    nestedField(10, "key", false, stringType(), null),
                                    nestedField(11, "value", false, stringType(), null)),
                                null),
                            false),
                        null),
                    nestedField(
                        12,
                        "fancy_properties",
                        false,
                        mapType(
                            13,
                            structType(
                                List.of(
                                    nestedField(
                                        14, "f1", false, listType(15, integerType(), false), null)),
                                null),
                            16,
                            structType(
                                List.of(
                                    nestedField(
                                        17,
                                        "struct",
                                        false,
                                        listType(
                                            18,
                                            structType(
                                                List.of(
                                                    nestedField(
                                                        19, "key", false, stringType(), null),
                                                    nestedField(
                                                        20, "value", false, stringType(), null)),
                                                null),
                                            false),
                                        null)),
                                null),
                            false),
                        null),
                    nestedField(
                        21,
                        "properties",
                        false,
                        mapType(22, stringType(), 23, stringType(), false),
                        null))
                .build(),
            // last field has a complex type with the highest ID 23 via a field-ID in a
            // non-nested-field type
            23));
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  public void addSchemaTableUpdates(int firstSchemaId) {
    var firstSchema =
        IcebergSchema.builder()
            .schemaId(firstSchemaId)
            .addFields(nestedField(100, "key", false, binaryType(), null))
            .build();
    var snapshot1 =
        new IcebergTableMetadataUpdateState(
                newIcebergTableSnapshot(UUID.randomUUID().toString()), ContentKey.of("foo"), false)
            .applyUpdates(
                List.of(
                    addSchema(firstSchema), setTrustedLocation("foo://bar/"), setCurrentSchema(-1)))
            .snapshot();
    var schemaByIcebergId = snapshot1.schemaByIcebergId();
    var expectedSchemaId = INITIAL_SCHEMA_ID;
    soft.assertThat(schemaByIcebergId).hasSize(1).containsKey(expectedSchemaId);
    soft.assertThat(snapshot1.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var secondSchema =
        IcebergSchema.builder()
            .schemaId(10)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null))
            .build();
    var snapshot2 =
        new IcebergTableMetadataUpdateState(snapshot1, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(secondSchema), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot2.schemaByIcebergId();
    expectedSchemaId = 1;
    soft.assertThat(schemaByIcebergId).hasSize(2).containsKey(expectedSchemaId);
    soft.assertThat(snapshot2.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var thirdSchema =
        IcebergSchema.builder()
            .schemaId(-1)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null),
                nestedField(300, "something", false, binaryType(), null))
            .build();
    var snapshot3 =
        new IcebergTableMetadataUpdateState(snapshot2, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(thirdSchema), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot3.schemaByIcebergId();
    expectedSchemaId = 2;
    soft.assertThat(schemaByIcebergId).hasSize(3).containsKey(expectedSchemaId);
    soft.assertThat(snapshot3.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var fourthSchemaExisting =
        IcebergSchema.builder()
            .schemaId(20)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null))
            .build();
    var snapshot4 =
        new IcebergTableMetadataUpdateState(snapshot3, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(fourthSchemaExisting), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot4.schemaByIcebergId();
    expectedSchemaId = 1;
    soft.assertThat(schemaByIcebergId).hasSize(3).containsKey(expectedSchemaId);
    soft.assertThat(snapshot4.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var fifthSchemaExisting =
        IcebergSchema.builder()
            .schemaId(-1)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null))
            .build();
    var snapshot5 =
        new IcebergTableMetadataUpdateState(snapshot4, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(fifthSchemaExisting), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot5.schemaByIcebergId();
    expectedSchemaId = 1;
    soft.assertThat(schemaByIcebergId).hasSize(3).containsKey(expectedSchemaId);
    soft.assertThat(snapshot5.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  public void addPartitionSpecUpdates(int firstSpecId) {
    var schema =
        IcebergSchema.builder()
            .schemaId(-1)
            .addFields(
                nestedField(100, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null),
                nestedField(300, "something", false, binaryType(), null))
            .build();
    var firstSpec =
        IcebergPartitionSpec.builder()
            .specId(firstSpecId)
            .addFields(partitionField("p_key", "identity", 1, 1050))
            .build();
    var snapshot1 =
        new IcebergTableMetadataUpdateState(
                newIcebergTableSnapshot(UUID.randomUUID().toString()), ContentKey.of("foo"), false)
            .applyUpdates(
                List.of(
                    addSchema(schema),
                    setTrustedLocation("foo://bar/"),
                    setCurrentSchema(-1),
                    addPartitionSpec(firstSpec),
                    setDefaultPartitionSpec(-1)))
            .snapshot();
    var specByIcebergId = snapshot1.partitionDefinitionByIcebergId();
    var expectedSpecId = INITIAL_SPEC_ID;
    soft.assertThat(specByIcebergId).hasSize(1).containsKey(expectedSpecId);
    soft.assertThat(snapshot1.currentPartitionDefinitionId())
        .isEqualTo(specByIcebergId.get(expectedSpecId).id());

    var secondSpec =
        IcebergPartitionSpec.builder()
            .specId(10)
            .addFields(partitionField("p_value", "identity", 2, 1051))
            .build();
    var snapshot2 =
        new IcebergTableMetadataUpdateState(snapshot1, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addPartitionSpec(secondSpec), setDefaultPartitionSpec(-1)))
            .snapshot();
    specByIcebergId = snapshot2.partitionDefinitionByIcebergId();
    expectedSpecId = 1;
    soft.assertThat(specByIcebergId).hasSize(2).containsKey(expectedSpecId);
    soft.assertThat(snapshot2.currentPartitionDefinitionId())
        .isEqualTo(specByIcebergId.get(expectedSpecId).id());

    var thirdSpec =
        IcebergPartitionSpec.builder()
            .specId(-1)
            .addFields(partitionField("p_something", "identity", 3, 1052))
            .build();
    var snapshot3 =
        new IcebergTableMetadataUpdateState(snapshot2, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addPartitionSpec(thirdSpec), setDefaultPartitionSpec(-1)))
            .snapshot();
    specByIcebergId = snapshot3.partitionDefinitionByIcebergId();
    expectedSpecId = 2;
    soft.assertThat(specByIcebergId).hasSize(3).containsKey(expectedSpecId);
    soft.assertThat(snapshot3.currentPartitionDefinitionId())
        .isEqualTo(specByIcebergId.get(expectedSpecId).id());

    var fourthSpecExisting =
        IcebergPartitionSpec.builder()
            .specId(20)
            .addFields(partitionField("p_value", "identity", 2, 1051))
            .build();
    var snapshot4 =
        new IcebergTableMetadataUpdateState(snapshot3, ContentKey.of("foo"), true)
            .applyUpdates(
                List.of(addPartitionSpec(fourthSpecExisting), setDefaultPartitionSpec(-1)))
            .snapshot();
    specByIcebergId = snapshot4.partitionDefinitionByIcebergId();
    expectedSpecId = 1;
    soft.assertThat(specByIcebergId).hasSize(3).containsKey(expectedSpecId);
    soft.assertThat(snapshot4.currentPartitionDefinitionId())
        .isEqualTo(specByIcebergId.get(expectedSpecId).id());

    var fifthSpecExisting =
        IcebergPartitionSpec.builder()
            .specId(-1)
            .addFields(partitionField("p_value", "identity", 2, 1051))
            .build();
    var snapshot5 =
        new IcebergTableMetadataUpdateState(snapshot4, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addPartitionSpec(fifthSpecExisting), setDefaultPartitionSpec(-1)))
            .snapshot();
    specByIcebergId = snapshot5.partitionDefinitionByIcebergId();
    expectedSpecId = 1;
    soft.assertThat(specByIcebergId).hasSize(3).containsKey(expectedSpecId);
    soft.assertThat(snapshot5.currentPartitionDefinitionId())
        .isEqualTo(specByIcebergId.get(expectedSpecId).id());

    var nonExistingField =
        IcebergPartitionSpec.builder()
            .specId(-1)
            .addFields(partitionField("p_value", "identity", 242, 1052))
            .build();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new IcebergTableMetadataUpdateState(snapshot4, ContentKey.of("foo"), true)
                    .applyUpdates(
                        List.of(addPartitionSpec(nonExistingField), setDefaultPartitionSpec(-1)))
                    .snapshot())
        .withMessage(
            "Source field with ID 242 not found for partition spec with ID 3 and transform 'identity'");
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  public void addSortTableUpdates(int firstSortId) {
    var schema =
        IcebergSchema.builder()
            .schemaId(-1)
            .addFields(
                nestedField(100, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null),
                nestedField(300, "something", false, binaryType(), null))
            .build();
    var firstSort =
        IcebergSortOrder.builder()
            .orderId(firstSortId)
            .addFields(sortField("identity", 1, ASC, NULLS_LAST))
            .build();
    var snapshot1 =
        new IcebergTableMetadataUpdateState(
                newIcebergTableSnapshot(UUID.randomUUID().toString()), ContentKey.of("foo"), false)
            .applyUpdates(
                List.of(
                    addSchema(schema),
                    setTrustedLocation("foo://bar/"),
                    setCurrentSchema(-1),
                    addSortOrder(firstSort),
                    setDefaultSortOrder(-1)))
            .snapshot();
    var sortByIcebergId = snapshot1.sortDefinitionByIcebergId();
    var expectedSortId = INITIAL_SORT_ORDER_ID;
    soft.assertThat(sortByIcebergId).hasSize(1).containsKey(expectedSortId);
    soft.assertThat(snapshot1.currentSortDefinitionId())
        .isEqualTo(sortByIcebergId.get(expectedSortId).id());

    var secondSort =
        IcebergSortOrder.builder()
            .orderId(10)
            .addFields(sortField("identity", 2, ASC, NULLS_LAST))
            .build();
    var snapshot2 =
        new IcebergTableMetadataUpdateState(snapshot1, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSortOrder(secondSort), setDefaultSortOrder(-1)))
            .snapshot();
    sortByIcebergId = snapshot2.sortDefinitionByIcebergId();
    expectedSortId = 2;
    soft.assertThat(sortByIcebergId).hasSize(2).containsKey(expectedSortId);
    soft.assertThat(snapshot2.currentSortDefinitionId())
        .isEqualTo(sortByIcebergId.get(expectedSortId).id());

    var thirdSort =
        IcebergSortOrder.builder()
            .orderId(-1)
            .addFields(sortField("identity", 3, ASC, NULLS_LAST))
            .build();
    var snapshot3 =
        new IcebergTableMetadataUpdateState(snapshot2, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSortOrder(thirdSort), setDefaultSortOrder(-1)))
            .snapshot();
    sortByIcebergId = snapshot3.sortDefinitionByIcebergId();
    expectedSortId = 3;
    soft.assertThat(sortByIcebergId).hasSize(3).containsKey(expectedSortId);
    soft.assertThat(snapshot3.currentSortDefinitionId())
        .isEqualTo(sortByIcebergId.get(expectedSortId).id());

    var fourthSortExisting =
        IcebergSortOrder.builder()
            .orderId(20)
            .addFields(sortField("identity", 2, ASC, NULLS_LAST))
            .build();
    var snapshot4 =
        new IcebergTableMetadataUpdateState(snapshot3, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSortOrder(fourthSortExisting), setDefaultSortOrder(-1)))
            .snapshot();
    sortByIcebergId = snapshot4.sortDefinitionByIcebergId();
    expectedSortId = 2;
    soft.assertThat(sortByIcebergId).hasSize(3).containsKey(expectedSortId);
    soft.assertThat(snapshot4.currentSortDefinitionId())
        .isEqualTo(sortByIcebergId.get(expectedSortId).id());

    var fifthSpecExisting =
        IcebergSortOrder.builder()
            .orderId(-1)
            .addFields(sortField("identity", 2, ASC, NULLS_LAST))
            .build();
    var snapshot5 =
        new IcebergTableMetadataUpdateState(snapshot4, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSortOrder(fifthSpecExisting), setDefaultSortOrder(-1)))
            .snapshot();
    sortByIcebergId = snapshot5.sortDefinitionByIcebergId();
    expectedSortId = 2;
    soft.assertThat(sortByIcebergId).hasSize(3).containsKey(expectedSortId);
    soft.assertThat(snapshot5.currentSortDefinitionId())
        .isEqualTo(sortByIcebergId.get(expectedSortId).id());

    var nonExistingField =
        IcebergSortOrder.builder()
            .orderId(-1)
            .addFields(sortField("identity", 42, ASC, NULLS_LAST))
            .build();
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new IcebergTableMetadataUpdateState(snapshot4, ContentKey.of("foo"), true)
                    .applyUpdates(List.of(addSortOrder(nonExistingField), setDefaultSortOrder(-1)))
                    .snapshot())
        .withMessage(
            "Iceberg field with ID 42 for sort order with ID 4 and transform 'identity' does not exist");
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  public void addSchemaViewUpdates(int firstSchemaId) {
    var firstSchema =
        IcebergSchema.builder()
            .schemaId(firstSchemaId)
            .addFields(nestedField(100, "key", false, binaryType(), null))
            .build();
    var snapshot1 =
        new IcebergViewMetadataUpdateState(
                newIcebergViewSnapshot(UUID.randomUUID().toString()), ContentKey.of("foo"), false)
            .applyUpdates(List.of(addSchema(firstSchema), setCurrentSchema(-1)))
            .snapshot();
    var schemaByIcebergId = snapshot1.schemaByIcebergId();
    var expectedSchemaId = INITIAL_SCHEMA_ID;
    soft.assertThat(schemaByIcebergId).hasSize(1).containsKey(expectedSchemaId);
    soft.assertThat(snapshot1.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var secondSchema =
        IcebergSchema.builder()
            .schemaId(10)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null))
            .build();
    var snapshot2 =
        new IcebergViewMetadataUpdateState(snapshot1, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(secondSchema), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot2.schemaByIcebergId();
    expectedSchemaId = 1;
    soft.assertThat(schemaByIcebergId).hasSize(2).containsKey(expectedSchemaId);
    soft.assertThat(snapshot2.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var thirdSchema =
        IcebergSchema.builder()
            .schemaId(-1)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null),
                nestedField(300, "something", false, binaryType(), null))
            .build();
    var snapshot3 =
        new IcebergViewMetadataUpdateState(snapshot2, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(thirdSchema), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot3.schemaByIcebergId();
    expectedSchemaId = 2;
    soft.assertThat(schemaByIcebergId).hasSize(3).containsKey(expectedSchemaId);
    soft.assertThat(snapshot3.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var fourthSchemaExisting =
        IcebergSchema.builder()
            .schemaId(20)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null))
            .build();
    var snapshot4 =
        new IcebergViewMetadataUpdateState(snapshot3, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(fourthSchemaExisting), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot4.schemaByIcebergId();
    expectedSchemaId = 1;
    soft.assertThat(schemaByIcebergId).hasSize(3).containsKey(expectedSchemaId);
    soft.assertThat(snapshot4.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());

    var fifthSchemaExisting =
        IcebergSchema.builder()
            .schemaId(-1)
            .addFields(
                nestedField(1, "key", false, binaryType(), null),
                nestedField(200, "value", false, binaryType(), null))
            .build();
    var snapshot5 =
        new IcebergViewMetadataUpdateState(snapshot4, ContentKey.of("foo"), true)
            .applyUpdates(List.of(addSchema(fifthSchemaExisting), setCurrentSchema(-1)))
            .snapshot();
    schemaByIcebergId = snapshot5.schemaByIcebergId();
    expectedSchemaId = 1;
    soft.assertThat(schemaByIcebergId).hasSize(3).containsKey(expectedSchemaId);
    soft.assertThat(snapshot5.currentSchemaId())
        .isEqualTo(schemaByIcebergId.get(expectedSchemaId).id());
  }

  @ParameterizedTest
  @MethodSource
  public void icebergNested(
      IcebergSchema schema, IcebergSchema expected, int expectedLastColumnId) {
    Map<Integer, Integer> remappedFields = new HashMap<>();
    IcebergSchema remapped = NessieModelIceberg.icebergInitialSchema(schema, remappedFields);

    soft.assertThat(remapped).isEqualTo(expected);

    String uuid = randomUUID().toString();

    NessieTableSnapshot snapshot =
        new IcebergTableMetadataUpdateState(
                newIcebergTableSnapshot(uuid), ContentKey.of("foo"), false)
            .applyUpdates(List.of(addSchema(schema), setTrustedLocation("foo://bar/")))
            .snapshot();
    soft.assertThat(snapshot)
        .extracting(NessieTableSnapshot::icebergLastColumnId)
        .isEqualTo(expectedLastColumnId);

    IcebergTableMetadata icebergMetadata =
        NessieModelIceberg.nessieTableSnapshotToIceberg(snapshot, Optional.empty(), m -> {});
    soft.assertThat(icebergMetadata)
        .extracting(IcebergTableMetadata::lastColumnId)
        .isEqualTo(expectedLastColumnId);
    soft.assertThat(
            icebergMetadata.schemas().stream()
                .filter(s -> s.schemaId() == requireNonNull(icebergMetadata.currentSchemaId()))
                .findFirst())
        .contains(expected);
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

  @Test
  public void testNessieSchemaToIcebergSchemaWithNestedIdentifier() {
    // Test for the fix: nested fields used as identifiers should not cause NullPointerException
    UUID topLevelFieldId = UUID.randomUUID();
    UUID nestedFieldId = UUID.randomUUID();
    UUID anotherNestedFieldId = UUID.randomUUID();

    // Create a schema with nested struct field
    NessieStruct struct =
        NessieStruct.nessieStruct(
            List.of(
                NessieField.builder()
                    .id(topLevelFieldId)
                    .icebergId(1)
                    .name("id")
                    .type(NessieType.intType())
                    .nullable(false)
                    .build(),
                NessieField.builder()
                    .id(UUID.randomUUID())
                    .icebergId(2)
                    .name("person")
                    .type(
                        NessieType.structType(
                            NessieStruct.nessieStruct(
                                List.of(
                                    NessieField.builder()
                                        .id(nestedFieldId)
                                        .icebergId(3)
                                        .name("name")
                                        .type(NessieType.stringType())
                                        .nullable(false)
                                        .build(),
                                    NessieField.builder()
                                        .id(anotherNestedFieldId)
                                        .icebergId(4)
                                        .name("age")
                                        .type(NessieType.intType())
                                        .nullable(true)
                                        .build()),
                                null)))
                    .nullable(false)
                    .build()),
            null);

    // Create schema with nested field as identifier (field ID 3 = person.name)
    NessieSchema nessieSchema =
        NessieSchema.nessieSchema(
            struct,
            0,
            List.of(topLevelFieldId, nestedFieldId)); // id and person.name as identifiers

    // Convert to Iceberg schema - this should not throw NullPointerException
    IcebergSchema icebergSchema = NessieModelIceberg.nessieSchemaToIcebergSchema(nessieSchema);

    // Verify identifier field IDs are correctly mapped
    soft.assertThat(icebergSchema.identifierFieldIds())
        .containsExactly(1, 3); // Iceberg field IDs for id and person.name
    soft.assertThat(icebergSchema.schemaId()).isEqualTo(0);
    soft.assertThat(icebergSchema.fields()).hasSize(2); // id and person
  }
}

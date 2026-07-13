/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.catalog.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromBytes;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.manifest.BooleanArray;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieFieldTransform;
import org.projectnessie.catalog.model.schema.NessieNullOrder;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessiePartitionField;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.schema.NessieSortDirection;
import org.projectnessie.catalog.model.schema.NessieSortField;
import org.projectnessie.catalog.model.schema.NessieStruct;
import org.projectnessie.catalog.model.schema.types.NessieType;
import org.projectnessie.catalog.model.schema.types.NessieTypeSpec;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewRepresentation;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.catalog.model.statistics.NessieIcebergBlobMetadata;
import org.projectnessie.catalog.model.statistics.NessieStatisticsFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestJackson3CatalogModelCompatibility {
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  private static final UUID FIELD_ID = UUID.fromString("8c4f7079-5905-4604-9333-e928d21613df");
  private static final NessieId SCHEMA_ID = nessieIdFromBytes(new byte[] {1, 2, 3, 4});
  private static final NessieId SNAPSHOT_ID = nessieIdFromBytes(new byte[] {5, 6, 7, 8});

  @Test
  void idAndBooleanArrayRoundTrip() throws Exception {
    NessieId id = nessieIdFromBytes(new byte[] {0, 1, 2, 3, 4});
    String idJson = MAPPER.writeValueAsString(id);
    assertThat(MAPPER.readValue(idJson, NessieId.class)).isEqualTo(id);

    BooleanArray booleanArray = new BooleanArray(6);
    booleanArray.set(0, Boolean.TRUE);
    booleanArray.set(1, Boolean.FALSE);
    booleanArray.set(4, Boolean.TRUE);

    String booleanArrayJson = MAPPER.writeValueAsString(booleanArray);
    BooleanArray deserialized = MAPPER.readValue(booleanArrayJson, BooleanArray.class);
    assertThat(deserialized.bytes()).isEqualTo(booleanArray.bytes());
  }

  @Test
  void schemaAndTypeSpecRoundTrip() throws Exception {
    NessieSchema schema = schema();

    String json = MAPPER.writeValueAsString(schema);
    JsonNode node = MAPPER.readValue(json, JsonNode.class);
    assertThat(node.get("struct").get("fields").get(0).get("type").get("type").asString())
        .isEqualTo("int");

    assertThat(MAPPER.readValue(json, NessieSchema.class)).isEqualTo(schema);

    NessieTypeSpec nestedType = NessieType.listType(NessieType.stringType(), 2, true);
    assertThat(MAPPER.readValue(MAPPER.writeValueAsString(nestedType), NessieTypeSpec.class))
        .isEqualTo(nestedType);
  }

  @Test
  void partitionAndSortDefinitionsRoundTrip() throws Exception {
    NessiePartitionDefinition partitionDefinition =
        NessiePartitionDefinition.nessiePartitionDefinition(
            List.of(
                NessiePartitionField.builder()
                    .id(UUID.randomUUID())
                    .sourceFieldId(FIELD_ID)
                    .name("id_bucket")
                    .type(NessieType.intType())
                    .transformSpec(NessieFieldTransform.bucket(16))
                    .icebergId(1000)
                    .build()),
            3);

    String partitionJson = MAPPER.writeValueAsString(partitionDefinition);
    JsonNode partitionNode = MAPPER.readValue(partitionJson, JsonNode.class);
    assertThat(partitionNode.get("fields").get(0).get("transformSpec").asString())
        .isEqualTo("bucket[16]");
    assertThat(MAPPER.readValue(partitionJson, NessiePartitionDefinition.class))
        .isEqualTo(partitionDefinition);

    NessieSortDefinition sortDefinition =
        NessieSortDefinition.nessieSortDefinition(
            List.of(
                NessieSortField.builder()
                    .sourceFieldId(FIELD_ID)
                    .type(NessieType.intType())
                    .transformSpec(NessieFieldTransform.identity())
                    .nullOrder(NessieNullOrder.NULLS_FIRST)
                    .direction(NessieSortDirection.ASC)
                    .build()),
            4);

    String sortJson = MAPPER.writeValueAsString(sortDefinition);
    JsonNode sortNode = MAPPER.readValue(sortJson, JsonNode.class);
    assertThat(sortNode.get("columns").get(0).get("direction").asString()).isEqualTo("asc");
    assertThat(MAPPER.readValue(sortJson, NessieSortDefinition.class)).isEqualTo(sortDefinition);
  }

  @Test
  void entityAndSnapshotRoundTrip() throws Exception {
    NessieTable table =
        NessieTable.builder()
            .nessieContentId("content-id")
            .icebergUuid("iceberg-uuid")
            .tableFormat(TableFormat.ICEBERG)
            .createdTimestamp(Instant.parse("2026-07-07T12:13:14Z"))
            .build();

    String entityJson = MAPPER.writeValueAsString(table);
    assertThat(MAPPER.readValue(entityJson, NessieEntity.class)).isEqualTo(table);

    NessieTableSnapshot snapshot =
        NessieTableSnapshot.builder()
            .id(SNAPSHOT_ID)
            .entity(table)
            .currentSchemaId(SCHEMA_ID)
            .addSchema(schema())
            .icebergFormatVersion(2)
            .lastUpdatedTimestamp(Instant.parse("2026-07-07T12:14:14Z"))
            .snapshotCreatedTimestamp(Instant.parse("2026-07-07T12:13:14Z"))
            .icebergLocation("s3://bucket/warehouse/ns/table")
            .addAdditionalKnownLocation("s3://bucket/warehouse/ns/table/data")
            .icebergLastPartitionId(1000)
            .addStatisticsFile(
                NessieStatisticsFile.statisticsFile(
                    "stats.puffin",
                    42L,
                    7L,
                    List.of(
                        NessieIcebergBlobMetadata.blobMetadata(
                            "apache-datasketches-theta-v1", 9L, List.of(1), Map.of("ndv", "10")))))
            .build();

    String snapshotJson = MAPPER.writeValueAsString(snapshot);
    assertThat(MAPPER.readValue(snapshotJson, NessieTableSnapshot.class)).isEqualTo(snapshot);
    assertThat(MAPPER.readValue(snapshotJson, NessieEntitySnapshot.class)).isEqualTo(snapshot);
  }

  @Test
  void viewRepresentationRoundTrip() throws Exception {
    NessieViewRepresentation representation =
        NessieViewRepresentation.NessieViewSQLRepresentation.nessieViewSQLRepresentation(
            "select * from ns.table", "spark");

    String json = MAPPER.writeValueAsString(representation);
    JsonNode node = MAPPER.readValue(json, JsonNode.class);
    assertThat(node.get("type").asString()).isEqualTo("sql");
    assertThat(MAPPER.readValue(json, NessieViewRepresentation.class)).isEqualTo(representation);
  }

  private static NessieSchema schema() {
    return NessieSchema.nessieSchema(
        SCHEMA_ID,
        NessieStruct.builder()
            .addField(
                NessieField.builder()
                    .id(FIELD_ID)
                    .name("id")
                    .type(NessieType.intType())
                    .nullable(false)
                    .icebergId(1)
                    .doc("identifier")
                    .build())
            .build(),
        1,
        List.of(FIELD_ID));
  }
}

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
package org.projectnessie.catalog.formats.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.partitionSpec;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField.sortField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.sortOrder;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.fixedType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.listType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.longType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergNamespace;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata;
import org.projectnessie.catalog.formats.iceberg.metrics.IcebergCommitReport;
import org.projectnessie.catalog.formats.iceberg.metrics.IcebergMetricsReport;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergConfigResponse;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergCreateNamespaceRequest;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergEndpoint;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateTableRequest;
import org.projectnessie.catalog.model.schema.NessieNullOrder;
import org.projectnessie.catalog.model.schema.NessieSortDirection;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class TestJackson3IcebergCompatibility {
  private static final ObjectMapper MAPPER = JsonMapper.builder().build();

  private static IcebergSchema schema() {
    return IcebergSchema.schema(
        42,
        List.of(1),
        List.of(
            nestedField(1, "id", true, longType(), null),
            nestedField(2, "tags", false, listType(3, stringType(), true), "tag list"),
            nestedField(4, "fixed", true, fixedType(16), null)));
  }

  @Test
  void icebergTypesRoundTrip() throws Exception {
    assertThat(MAPPER.writeValueAsString(longType())).isEqualTo("\"long\"");
    assertThat(
            MAPPER.readValue(
                "\"long\"", org.projectnessie.catalog.formats.iceberg.types.IcebergType.class))
        .isEqualTo(longType());

    var listType = listType(44, stringType(), true);
    String json = MAPPER.writeValueAsString(listType);
    assertThat(MAPPER.readValue(json, JsonNode.class).get("element-id").intValue()).isEqualTo(44);
    assertThat(
            MAPPER.readValue(
                json, org.projectnessie.catalog.formats.iceberg.types.IcebergType.class))
        .isEqualTo(listType);
  }

  @Test
  void metadataRoundTrip() throws Exception {
    IcebergTableMetadata metadata =
        IcebergTableMetadata.builder()
            .formatVersion(2)
            .tableUuid("table-uuid")
            .location("file:///warehouse/ns/table")
            .lastSequenceNumber(7L)
            .lastUpdatedMs(123L)
            .lastColumnId(4)
            .currentSchemaId(42)
            .defaultSpecId(3)
            .lastPartitionId(1000)
            .defaultSortOrderId(2)
            .addSchema(schema())
            .addPartitionSpec(
                partitionSpec(3, List.of(partitionField("id_bucket", "bucket[16]", 1, 1000))))
            .addSortOrder(
                sortOrder(
                    2,
                    List.of(
                        sortField(
                            "identity", 1, NessieSortDirection.ASC, NessieNullOrder.NULLS_FIRST))))
            .putProperty("owner", "nessie")
            .build();

    String json = MAPPER.writeValueAsString(metadata);
    JsonNode node = MAPPER.readValue(json, JsonNode.class);
    assertThat(node.get("format-version").intValue()).isEqualTo(2);
    assertThat(node.get("last-sequence-number").longValue()).isEqualTo(7L);
    assertThat(
            node.get("schemas").get(0).get("fields").get(1).get("type").get("element").asString())
        .isEqualTo("string");

    assertThat(MAPPER.readValue(json, IcebergTableMetadata.class)).isEqualTo(metadata);
  }

  @Test
  void restPayloadsRoundTrip() throws Exception {
    IcebergConfigResponse config =
        IcebergConfigResponse.builder()
            .putDefault("warehouse", "s3://bucket")
            .putOverride("client.region", "eu-central-1")
            .addEndpoint(IcebergEndpoint.icebergEndpoint("GET", "/v1/config"))
            .build();

    String configJson = MAPPER.writeValueAsString(config);
    JsonNode configNode = MAPPER.readValue(configJson, JsonNode.class);
    assertThat(configNode.get("endpoints").get(0).asString()).isEqualTo("GET /v1/config");
    assertThat(MAPPER.readValue(configJson, IcebergConfigResponse.class)).isEqualTo(config);

    IcebergCreateNamespaceRequest createNamespace =
        IcebergCreateNamespaceRequest.builder()
            .namespace(IcebergNamespace.icebergNamespace(List.of("a", "b")))
            .putProperty("owner", "nessie")
            .build();
    String namespaceJson = MAPPER.writeValueAsString(createNamespace);
    assertThat(MAPPER.readValue(namespaceJson, JsonNode.class).get("namespace").get(0).asString())
        .isEqualTo("a");
    assertThat(MAPPER.readValue(namespaceJson, IcebergCreateNamespaceRequest.class))
        .isEqualTo(createNamespace);
  }

  @Test
  void metadataUpdateAndRequirementPolymorphism() throws Exception {
    String updateJson = "{\"action\":\"assign-uuid\",\"uuid\":\"table-uuid\"}";
    IcebergMetadataUpdate update = MAPPER.readValue(updateJson, IcebergMetadataUpdate.class);
    assertThat(update).isInstanceOf(IcebergMetadataUpdate.AssignUUID.class);
    assertThat(
            MAPPER
                .readValue(MAPPER.writeValueAsString(update), JsonNode.class)
                .get("action")
                .asString())
        .isEqualTo("assign-uuid");

    String requirementJson = "{\"type\":\"assert-table-uuid\",\"uuid\":\"table-uuid\"}";
    IcebergUpdateRequirement requirement =
        MAPPER.readValue(requirementJson, IcebergUpdateRequirement.class);
    assertThat(requirement).isInstanceOf(IcebergUpdateRequirement.AssertTableUUID.class);
    assertThat(
            MAPPER
                .readValue(MAPPER.writeValueAsString(requirement), JsonNode.class)
                .get("type")
                .asString())
        .isEqualTo("assert-table-uuid");

    IcebergUpdateTableRequest request =
        IcebergUpdateTableRequest.builder()
            .identifier(
                IcebergTableIdentifier.icebergTableIdentifier(
                    IcebergNamespace.icebergNamespace(List.of("ns")), "table"))
            .addRequirement(requirement)
            .addUpdate(update)
            .build();
    assertThat(
            MAPPER.readValue(MAPPER.writeValueAsString(request), IcebergUpdateTableRequest.class))
        .isEqualTo(request);
  }

  @Test
  void metricsRoundTrip() throws Exception {
    String json =
        """
        {
          "report-type": "commit-report",
          "table-name": "ns.table",
          "snapshot-id": 12,
          "sequence-number": 3,
          "operation": "append",
          "metrics": {
            "total-duration": {
              "count": 1,
              "time-unit": "nanoseconds",
              "total-duration": 42
            },
            "added-records": {
              "unit": "count",
              "value": 2
            }
          },
          "metadata": {
            "iceberg-version": "test"
          }
        }
        """;

    IcebergMetricsReport deserialized = MAPPER.readValue(json, IcebergMetricsReport.class);
    assertThat(deserialized).isInstanceOf(IcebergCommitReport.class);
    IcebergCommitReport report = (IcebergCommitReport) deserialized;
    assertThat(report.metrics().totalDuration().timeUnit()).isEqualTo(TimeUnit.NANOSECONDS);

    json = MAPPER.writeValueAsString(report);
    JsonNode node = MAPPER.readValue(json, JsonNode.class);
    assertThat(node.get("report-type").asString()).isEqualTo("commit-report");
    assertThat(node.get("metrics").get("total-duration").get("time-unit").asString())
        .isEqualTo("nanoseconds");

    assertThat(MAPPER.readValue(json, IcebergMetricsReport.class)).isEqualTo(report);
  }
}

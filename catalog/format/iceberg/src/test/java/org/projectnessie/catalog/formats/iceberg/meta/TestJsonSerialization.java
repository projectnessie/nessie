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
package org.projectnessie.catalog.formats.iceberg.meta;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergJson.objectMapper;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.partitionSpec;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot.snapshot;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotRef.snapshotRef;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField.sortField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.sortOrder;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.INITIAL_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.fixedType;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.longType;

import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.model.schema.NessieNullOrder;
import org.projectnessie.catalog.model.schema.NessieSortDirection;

@ExtendWith(SoftAssertionsExtension.class)
public class TestJsonSerialization {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void tableMetadata(IcebergTableMetadata tableMetadata) throws Exception {
    IcebergSpec spec = IcebergSpec.forVersion(2);
    String json = spec.jsonWriter().writeValueAsString(tableMetadata);
    IcebergTableMetadata deserialized = objectMapper().readValue(json, IcebergTableMetadata.class);
    soft.assertThat(deserialized).isEqualTo(tableMetadata);

    TableMetadata metadata = TableMetadataParser.fromJson(json);
    String metadataJson = TableMetadataParser.toJson(metadata);

    IcebergTableMetadata deserialized2 =
        spec.jsonReader().readValue(metadataJson, IcebergTableMetadata.class);
    soft.assertThat(deserialized2).isEqualTo(tableMetadata);
  }

  static Stream<IcebergTableMetadata> tableMetadata() {
    return Stream.of(
        IcebergTableMetadata.builder()
            .formatVersion(2)
            .lastUpdatedMs(6666666L)
            .lastColumnId(400)
            .currentSchemaId(42)
            .defaultSpecId(3)
            .lastPartitionId(INITIAL_PARTITION_ID)
            .defaultSortOrderId(2)
            .currentSnapshotId(123456L)
            .location("file:///blah/blah")
            .lastSequenceNumber(333L)
            .tableUuid(UUID.randomUUID().toString())
            .addSchemas(
                IcebergSchema.schema(
                    42,
                    asList(1, 2),
                    asList(
                        nestedField(1, "one", true, longType(), null),
                        nestedField(2, "two", true, fixedType(16), null))))
            .addPartitionSpecs(
                partitionSpec(
                    3, singletonList(partitionField("one_bucket", "bucket[16]", 1, 1111))))
            .addSortOrders(
                sortOrder(
                    2,
                    singletonList(
                        sortField(
                            "identity", 2, NessieSortDirection.ASC, NessieNullOrder.NULLS_FIRST))),
                sortOrder(
                    3,
                    singletonList(
                        sortField(
                            "identity", 1, NessieSortDirection.DESC, NessieNullOrder.NULLS_LAST))))
            .addSnapshots(
                snapshot(
                    333L,
                    123456L,
                    null,
                    6666666L,
                    singletonMap("operation", "append"),
                    emptyList(),
                    "file:///blah/blah/manifest-list",
                    42))
            .putRef("main", snapshotRef(123456L, "branch", null, null, null))
            .build());
  }
}

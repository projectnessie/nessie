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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergNestedField.nestedField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionField.partitionField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergPartitionSpec.partitionSpec;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSchema.schema;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot.snapshot;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshotRef.snapshotRef;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortField.sortField;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergSortOrder.sortOrder;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.INITIAL_PARTITION_ID;
import static org.projectnessie.catalog.formats.iceberg.types.IcebergType.stringType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.projectnessie.catalog.formats.iceberg.IcebergSpec;
import org.projectnessie.catalog.model.schema.NessieNullOrder;
import org.projectnessie.catalog.model.schema.NessieSortDirection;

@Warmup(iterations = 2, time = 2000, timeUnit = MILLISECONDS)
@Measurement(iterations = 3, time = 1000, timeUnit = MILLISECONDS)
@Fork(1)
@Threads(4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(MICROSECONDS)
public class JsonSerializeBench {
  @State(Scope.Benchmark)
  public static class BenchmarkParam {
    @Param({"10", "100", "1000", "5000"})
    public int schemaFields;

    @Param({"1", "10", "100"})
    public int schemaVersions;

    @Param({"1"})
    public int partitionSpecFields;

    @Param({"1", "10"})
    public int partitionSpecVersions;

    @Param({"2"})
    public int sortOrderFields;

    @Param({"1", "10", "100"})
    public int sortOrderVersions;

    @Param({"1", "10", "100"})
    public int snapshotCount;

    IcebergTableMetadata icebergTableMetadata;
    TableMetadata tableMetadata;
    String json;

    @Setup
    public void init() throws Exception {
      List<IcebergNestedField> fields = new ArrayList<>(schemaFields);

      IcebergTableMetadata.Builder builder =
          IcebergTableMetadata.builder()
              .formatVersion(2)
              .lastUpdatedMs(6666666L)
              .lastColumnId(400)
              .currentSchemaId(schemaVersions)
              .defaultSpecId(partitionSpecVersions)
              .lastPartitionId(INITIAL_PARTITION_ID)
              .defaultSortOrderId(sortOrderVersions)
              .currentSnapshotId(100000L + snapshotCount - 1)
              .location("file:///blah/blah")
              .lastSequenceNumber(333L)
              .tableUuid(UUID.randomUUID().toString())
              .putRef("main", snapshotRef(100000L + snapshotCount - 1, "branch", null, null, null));

      for (int sv = 1; sv <= schemaVersions; sv++) {
        int fieldOffset = 1000 * sv;
        for (int f = 0; f < schemaFields; f++) {
          fields.add(
              nestedField(
                  fieldOffset + f,
                  "f_" + f + "_sv" + sv,
                  f < partitionSpecFields,
                  stringType(),
                  "doc for field " + f));
        }
        List<Integer> identifierFields =
            IntStream.range(0, partitionSpecFields)
                .map(i -> fieldOffset + i)
                .boxed()
                .collect(Collectors.toList());
        builder.addSchemas(schema(sv, identifierFields, fields));
      }

      for (int pv = 1; pv <= partitionSpecVersions; pv++) {
        List<IcebergPartitionField> partitionFields = new ArrayList<>(partitionSpecFields);
        int fieldOffset = 1000 * pv;
        for (int f = 0; f < partitionSpecFields; f++) {
          partitionFields.add(
              partitionField("f_" + f + "_sv" + pv, "identity", fieldOffset, fieldOffset + f));
        }
        builder.addPartitionSpecs(partitionSpec(pv, partitionFields));
      }

      for (int sv = 1; sv <= sortOrderVersions; sv++) {
        List<IcebergSortField> sortFields = new ArrayList<>(sortOrderFields);
        int fieldOffset = 1000 * sv;
        for (int f = 0; f < sortOrderFields; f++) {
          sortFields.add(
              sortField(
                  "identity", fieldOffset, NessieSortDirection.ASC, NessieNullOrder.NULLS_FIRST));
        }
        builder.addSortOrders(sortOrder(sv, sortFields));
      }

      for (int snap = 0; snap < snapshotCount; snap++) {
        builder.addSnapshots(
            snapshot(
                333L,
                100000L + snap,
                null,
                6666666L,
                singletonMap("operation", "append"),
                emptyList(),
                "file:///blah/blah/manifest-list",
                1));
      }

      icebergTableMetadata = builder.build();

      json =
          IcebergSpec.forVersion(icebergTableMetadata.formatVersion())
              .jsonWriter()
              .writeValueAsString(icebergTableMetadata);

      tableMetadata = TableMetadataParser.fromJson(json);
    }
  }

  @Benchmark
  public String serializeIcebergTableMetadata(BenchmarkParam param) throws IOException {
    return IcebergSpec.forVersion(param.tableMetadata.formatVersion())
        .jsonWriter()
        .writeValueAsString(param.icebergTableMetadata);
  }

  @Benchmark
  public String serializeTableMetadata(BenchmarkParam param) {
    return TableMetadataParser.toJson(param.tableMetadata);
  }

  @Benchmark
  public IcebergTableMetadata deserializeIcebergTableMetadata(BenchmarkParam param)
      throws IOException {
    return IcebergSpec.forVersion(param.tableMetadata.formatVersion())
        .jsonReader()
        .readValue(param.json, IcebergTableMetadata.class);
  }

  @Benchmark
  public TableMetadata deserializeTableMetadata(BenchmarkParam param) {
    return TableMetadataParser.fromJson(param.json);
  }
}

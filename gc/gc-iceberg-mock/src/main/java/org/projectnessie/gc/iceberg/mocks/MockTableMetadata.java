/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.gc.iceberg.mocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.io.FileIO;
import org.immutables.value.Value;

/**
 * Generates mocked <em>Iceberg table metadata</em> objects based on a bunch of parameters.
 *
 * <p>Parameters define the number of snapshots and the number of {@link ManifestContent#DATA} and
 * {@link ManifestContent#DELETES} manifests for each snapshot, using {@link
 * Snapshot#manifestListLocation()}.
 *
 * <p>Also generates the appropriate {@link org.apache.iceberg.ManifestEntry}s for {@link
 * Snapshot#addedDataFiles(FileIO)} and {@link Snapshot#removedDeleteFiles(FileIO)}.
 */
@Value.Immutable
public abstract class MockTableMetadata {

  public static final int FORMAT_VERSION = 2;

  public static MockTableMetadata empty() {
    return ImmutableMockTableMetadata.builder().location("nope://").build();
  }

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("format-version", FORMAT_VERSION)
        .put("table-uuid", tableUuid())
        .put("location", location())
        .put("last-sequence-number", lastSequenceNumber())
        .put("last-updated-ms", lastUpdatedMs())
        .put("last-column-id", lastColumnId())
        .put("current-schema-id", currentSchemaId());
    schemas(node.putArray("schemas"));
    node.put("default-spec-id", defaultSpecId());
    partitionSpecs(node.putArray("partition-specs"));
    node.put("last-partition-id", lastPartitionId())
        .put("default-sort-order-id", defaultSortOrderId());
    sortOrders(node.putArray("sort-orders"));
    node.putObject("properties");
    node.put("current-snapshot-id", currentSnapshotId());
    snapshots(node.putArray("snapshots"));
    node.putArray("snapshot-log");
    node.putArray("metadata-log");
    return node;
  }

  public abstract List<MockSnapshot> snapshots();

  @Value.Default
  public List<MockSchema> schemas() {
    return Collections.singletonList(ImmutableMockSchema.builder().build());
  }

  @Value.Default
  public List<MockSortOrder> sortOrders() {
    return Collections.singletonList(ImmutableMockSortOrder.builder().build());
  }

  @Value.Default
  public List<MockPartitionSpec> partitionSpecs() {
    return Collections.singletonList(ImmutableMockPartitionSpec.builder().build());
  }

  private void schemas(ArrayNode schemas) {
    for (MockSchema schema : schemas()) {
      schemas.add(schema.jsonNode());
    }
  }

  private void partitionSpecs(ArrayNode partitionSpecs) {
    for (MockPartitionSpec partitionSpec : partitionSpecs()) {
      partitionSpecs.add(partitionSpec.jsonNode());
    }
  }

  private void sortOrders(ArrayNode sortOrders) {
    for (MockSortOrder sortOrder : sortOrders()) {
      sortOrders.add(sortOrder.jsonNode());
    }
  }

  private void snapshots(ArrayNode snapshots) {
    for (MockSnapshot snapshot : snapshots()) {
      snapshots.add(snapshot.jsonNode());
    }
  }

  @Value.Default
  public long currentSnapshotId() {
    return 0;
  }

  @Value.Default
  public int defaultSortOrderId() {
    return 0;
  }

  @Value.Default
  public int lastPartitionId() {
    return 0;
  }

  @Value.Default
  public int defaultSpecId() {
    return 0;
  }

  @Value.Default
  public int currentSchemaId() {
    return 0;
  }

  @Value.Default
  public int lastColumnId() {
    return 0;
  }

  @Value.Default
  public long lastUpdatedMs() {
    return 0;
  }

  @Value.Default
  public int lastSequenceNumber() {
    return 0;
  }

  public abstract String location();

  @Value.Default
  public String tableUuid() {
    return UUID.randomUUID().toString();
  }

  public MockPartitionSpec partitionSpec(int partitionSpecId) {
    for (MockPartitionSpec partitionSpec : partitionSpecs()) {
      if (partitionSpec.specId() == partitionSpecId) {
        return partitionSpec;
      }
    }
    throw new IllegalArgumentException();
  }

  public MockSchema schema(int schemaId) {
    for (MockSchema schema : schemas()) {
      if (schema.schemaId() == schemaId) {
        return schema;
      }
    }
    throw new IllegalArgumentException();
  }
}

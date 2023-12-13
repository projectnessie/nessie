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

import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.dataFileBase;
import static org.projectnessie.gc.iceberg.mocks.IcebergFileIOMocking.manifestFileLocation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.Nullable;
import java.util.stream.Stream;
import org.apache.iceberg.io.OutputFile;
import org.immutables.value.Value;

@Value.Immutable
public abstract class MockSnapshot {

  @Value.Lazy
  public JsonNode jsonNode() {
    ObjectNode node = new ObjectNode(JsonNodeFactory.instance);
    node.put("sequence-number", sequenceNumber())
        .put("snapshot-id", snapshotId())
        .put("timestamp-ms", timestampMs());
    summary(node.putObject("summary"));
    if (manifestListLocation() != null) {
      node.put("manifest-list", manifestListLocation());
    }
    node.putArray("manifests");
    return node;
  }

  @Nullable
  public abstract String manifestListLocation();

  @Value.Default
  public long sequenceNumber() {
    return 0L;
  }

  @Value.Default
  public long snapshotId() {
    return 0L;
  }

  @Value.Default
  public long timestampMs() {
    return 0L;
  }

  @Value.Default
  public String summaryOperation() {
    return "snapshot-operation";
  }

  private void summary(ObjectNode summary) {
    summary.put("operation", summaryOperation());
  }

  @Value.Default
  public MockPartitionSpec partitionSpec() {
    return MockPartitionSpec.DEFAULT_EMPTY;
  }

  @Value.Default
  public MockSchema schema() {
    return MockSchema.DEFAULT_EMPTY;
  }

  public abstract String tableUuid();

  @Value.Auxiliary
  public Stream<MockManifestFile> manifestFiles() {
    return Stream.of(
        ImmutableMockManifestFile.builder()
            .path(manifestFileLocation(tableUuid(), snapshotId(), 0))
            .addedFilesCount(1)
            .addedRowsCount(1L)
            .snapshotId(snapshotId())
            .partitionSpecId(partitionSpec().specId())
            .baseDataFilePath(dataFileBase(tableUuid(), snapshotId(), 0))
            .build());
  }

  public void generateManifestList(OutputFile output) {
    ImmutableMockManifestList.Builder manifestList = ImmutableMockManifestList.builder();
    manifestFiles().forEach(manifestList::addManifestFiles);
    manifestList.build().write(output, snapshotId(), -1L, 0L);
  }
}

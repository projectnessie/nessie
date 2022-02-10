/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.server.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.store.ObjectTypes.IcebergMetadataPointer;
import org.projectnessie.store.ObjectTypes.IcebergRefState;
import org.projectnessie.store.ObjectTypes.IcebergViewState;

class TestStoreWorker {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ID = "x";
  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testDeserialization(Map.Entry<ByteString, Content> entry) {
    Content actual = worker.valueFromStore(entry.getKey(), Optional.empty());
    Assertions.assertEquals(entry.getValue(), actual);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerialization(Map.Entry<ByteString, Content> entry) {
    ByteString actual = worker.toStoreOnReferenceState(entry.getValue());
    Assertions.assertEquals(entry.getKey(), actual);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerde(Map.Entry<ByteString, Content> entry) {
    ByteString actualBytes = worker.toStoreOnReferenceState(entry.getValue());
    Assertions.assertEquals(entry.getValue(), worker.valueFromStore(actualBytes, Optional.empty()));
    Content actualContent = worker.valueFromStore(entry.getKey(), Optional.empty());
    Assertions.assertEquals(entry.getKey(), worker.toStoreOnReferenceState(actualContent));
  }

  @Test
  void testSerdeIceberg() {
    String path = "foo/bar";
    IcebergTable table = IcebergTable.of(path, 42, 43, 44, 45, ID);

    ObjectTypes.Content protoTableGlobal =
        ObjectTypes.Content.newBuilder()
            .setId(ID)
            .setIcebergMetadataPointer(
                IcebergMetadataPointer.newBuilder().setMetadataLocation(path))
            .build();
    ObjectTypes.Content protoOnRef =
        ObjectTypes.Content.newBuilder()
            .setId(ID)
            .setIcebergRefState(
                IcebergRefState.newBuilder()
                    .setSnapshotId(42)
                    .setSchemaId(43)
                    .setSpecId(44)
                    .setSortOrderId(45))
            .build();

    ByteString tableGlobalBytes = worker.toStoreGlobalState(table);
    ByteString snapshotBytes = worker.toStoreOnReferenceState(table);

    Assertions.assertEquals(protoTableGlobal.toByteString(), tableGlobalBytes);
    Assertions.assertEquals(protoOnRef.toByteString(), snapshotBytes);

    Content deserialized = worker.valueFromStore(snapshotBytes, Optional.of(tableGlobalBytes));
    Assertions.assertEquals(table, deserialized);
  }

  @Test
  void testSerdeIcebergView() {
    String path = "foo/view";
    String dialect = "Dremio";
    String sqlText = "select * from world";
    IcebergView view = IcebergView.of(ID, path, 1, 123, dialect, sqlText);

    ObjectTypes.Content protoTableGlobal =
        ObjectTypes.Content.newBuilder()
            .setId(ID)
            .setIcebergMetadataPointer(
                IcebergMetadataPointer.newBuilder().setMetadataLocation(path))
            .build();
    ObjectTypes.Content protoOnRef =
        ObjectTypes.Content.newBuilder()
            .setId(ID)
            .setIcebergViewState(
                IcebergViewState.newBuilder()
                    .setVersionId(1)
                    .setDialect(dialect)
                    .setSchemaId(123)
                    .setSqlText(sqlText))
            .build();

    ByteString tableGlobalBytes = worker.toStoreGlobalState(view);
    ByteString snapshotBytes = worker.toStoreOnReferenceState(view);

    Assertions.assertEquals(protoTableGlobal.toByteString(), tableGlobalBytes);
    Assertions.assertEquals(protoOnRef.toByteString(), snapshotBytes);

    Content deserialized = worker.valueFromStore(snapshotBytes, Optional.of(tableGlobalBytes));
    Assertions.assertEquals(view, deserialized);
  }

  @Test
  void testCommitSerde() throws JsonProcessingException {
    CommitMeta expectedCommit =
        ImmutableCommitMeta.builder()
            .commitTime(Instant.now())
            .authorTime(Instant.now())
            .author("bill")
            .committer("ted")
            .hash("xyz")
            .message("commit msg")
            .build();

    ByteString expectedBytes = ByteString.copyFrom(MAPPER.writeValueAsBytes(expectedCommit));
    CommitMeta actualCommit = worker.getMetadataSerializer().fromBytes(expectedBytes);
    Assertions.assertEquals(expectedCommit, actualCommit);
    ByteString actualBytes = worker.getMetadataSerializer().toBytes(expectedCommit);
    Assertions.assertEquals(expectedBytes, actualBytes);
    actualBytes = worker.getMetadataSerializer().toBytes(expectedCommit);
    Assertions.assertEquals(expectedCommit, worker.getMetadataSerializer().fromBytes(actualBytes));
    actualCommit = worker.getMetadataSerializer().fromBytes(expectedBytes);
    Assertions.assertEquals(expectedBytes, worker.getMetadataSerializer().toBytes(actualCommit));
  }

  private static Stream<Map.Entry<ByteString, Content>> provideDeserialization() {
    return Stream.of(getDelta(), getNamespace());
  }

  private static Map.Entry<ByteString, Content> getDelta() {
    String path = "foo/bar";
    String cl1 = "xyz";
    String cl2 = "abc";
    String ml1 = "efg";
    String ml2 = "hij";
    Content content =
        ImmutableDeltaLakeTable.builder()
            .lastCheckpoint(path)
            .addCheckpointLocationHistory(cl1)
            .addCheckpointLocationHistory(cl2)
            .addMetadataLocationHistory(ml1)
            .addMetadataLocationHistory(ml2)
            .id(ID)
            .build();
    ByteString bytes =
        ObjectTypes.Content.newBuilder()
            .setId(ID)
            .setDeltaLakeTable(
                ObjectTypes.DeltaLakeTable.newBuilder()
                    .setLastCheckpoint(path)
                    .addCheckpointLocationHistory(cl1)
                    .addCheckpointLocationHistory(cl2)
                    .addMetadataLocationHistory(ml1)
                    .addMetadataLocationHistory(ml2))
            .build()
            .toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, content);
  }

  private static Map.Entry<ByteString, Content> getNamespace() {
    String name = "a.b.c";
    Content content = ImmutableNamespace.builder().id(name).name(name).build();
    ByteString bytes =
        ObjectTypes.Content.newBuilder()
            .setId(name)
            .setNamespace(ObjectTypes.Namespace.newBuilder().setName(name).build())
            .build()
            .toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, content);
  }
}

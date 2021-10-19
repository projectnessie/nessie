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
import org.projectnessie.model.Contents;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.SqlView;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.store.ObjectTypes.IcebergGlobal;
import org.projectnessie.store.ObjectTypes.IcebergMetadataPointer;

class TestStoreWorker {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ID = "x";
  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testDeserialization(Map.Entry<ByteString, Contents> entry) {
    Contents actual = worker.valueFromStore(entry.getKey(), Optional.empty());
    Assertions.assertEquals(entry.getValue(), actual);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerialization(Map.Entry<ByteString, Contents> entry) {
    ByteString actual = worker.toStoreOnReferenceState(entry.getValue());
    Assertions.assertEquals(entry.getKey(), actual);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerde(Map.Entry<ByteString, Contents> entry) {
    ByteString actualBytes = worker.toStoreOnReferenceState(entry.getValue());
    Assertions.assertEquals(entry.getValue(), worker.valueFromStore(actualBytes, Optional.empty()));
    Contents actualContents = worker.valueFromStore(entry.getKey(), Optional.empty());
    Assertions.assertEquals(entry.getKey(), worker.toStoreOnReferenceState(actualContents));
  }

  @Test
  void testSerdeIceberg() {
    String path = "foo/bar";
    IcebergTable table = IcebergTable.of(path, "xyz", ID);

    ObjectTypes.Contents protoTableGlobal =
        ObjectTypes.Contents.newBuilder()
            .setId(ID)
            .setIcebergGlobal(IcebergGlobal.newBuilder().setIdGenerators("xyz"))
            .build();
    ObjectTypes.Contents protoOnRef =
        ObjectTypes.Contents.newBuilder()
            .setId(ID)
            .setIcebergMetadataPointer(
                IcebergMetadataPointer.newBuilder().setMetadataLocation(path))
            .build();

    ByteString tableGlobalBytes = worker.toStoreGlobalState(table);
    ByteString snapshotBytes = worker.toStoreOnReferenceState(table);

    Assertions.assertEquals(protoTableGlobal.toByteString(), tableGlobalBytes);
    Assertions.assertEquals(protoOnRef.toByteString(), snapshotBytes);

    Contents deserialized = worker.valueFromStore(snapshotBytes, Optional.of(tableGlobalBytes));
    Assertions.assertEquals(table, deserialized);
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

  private static Stream<Map.Entry<ByteString, Contents>> provideDeserialization() {
    return Stream.of(getDelta(), getView());
  }

  private static Map.Entry<ByteString, Contents> getDelta() {
    String path = "foo/bar";
    String cl1 = "xyz";
    String cl2 = "abc";
    String ml1 = "efg";
    String ml2 = "hij";
    Contents contents =
        ImmutableDeltaLakeTable.builder()
            .lastCheckpoint(path)
            .addCheckpointLocationHistory(cl1)
            .addCheckpointLocationHistory(cl2)
            .addMetadataLocationHistory(ml1)
            .addMetadataLocationHistory(ml2)
            .id(ID)
            .build();
    ByteString bytes =
        ObjectTypes.Contents.newBuilder()
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
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }

  private static Map.Entry<ByteString, Contents> getView() {
    String path = "SELECT * FROM foo.bar,";
    Contents contents =
        ImmutableSqlView.builder().dialect(SqlView.Dialect.DREMIO).sqlText(path).id(ID).build();
    ByteString bytes =
        ObjectTypes.Contents.newBuilder()
            .setId(ID)
            .setSqlView(ObjectTypes.SqlView.newBuilder().setSqlText(path).setDialect("DREMIO"))
            .build()
            .toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }
}

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

import java.time.Instant;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableHiveDatabase;
import org.projectnessie.model.ImmutableHiveTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.SqlView;
import org.projectnessie.store.ObjectTypes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

class TestStoreWorker {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String ID = "x";
  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testDeserialization(Map.Entry<ByteString, Contents> entry) {
    Contents actual = worker.getValueSerializer().fromBytes(entry.getKey());
    Assertions.assertEquals(entry.getValue(), actual);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerialization(Map.Entry<ByteString, Contents> entry) {
    ByteString actual = worker.getValueSerializer().toBytes(entry.getValue());
    Assertions.assertEquals(entry.getKey(), actual);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerde(Map.Entry<ByteString, Contents> entry) {
    ByteString actualBytes = worker.getValueSerializer().toBytes(entry.getValue());
    Assertions.assertEquals(entry.getValue(), worker.getValueSerializer().fromBytes(actualBytes));
    Contents actualContents = worker.getValueSerializer().fromBytes(entry.getKey());
    Assertions.assertEquals(entry.getKey(), worker.getValueSerializer().toBytes(actualContents));
  }

  @Test
  void testCommitSerde() throws JsonProcessingException {
    CommitMeta expectedCommit = ImmutableCommitMeta.builder().commitTime(Instant.now())
        .author("bill")
        .authorTime(Instant.now())
        .author("ted")
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
    return Stream.of(
      getIceberg(),
      getHiveDb(),
      getHiveTable(),
      getDelta(),
      getView()
    );
  }

  private static Map.Entry<ByteString, Contents> getIceberg() {
    String path = "foo/bar";
    Contents contents = ImmutableIcebergTable.builder().metadataLocation(path).id(ID).build();
    ByteString bytes = ObjectTypes.Contents.newBuilder().setId(ID)
        .setIcebergTable(ObjectTypes.IcebergTable.newBuilder().setMetadataLocation(path)).build().toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }

  private static Map.Entry<ByteString, Contents> getHiveDb() {
    byte[] database = new byte[]{0, 1, 2, 3, 4, 5};
    Contents contents = ImmutableHiveDatabase.builder().databaseDefinition(database).id(ID).build();
    ByteString bytes = ObjectTypes.Contents.newBuilder().setId(ID)
      .setHiveDatabase(ObjectTypes.HiveDatabase.newBuilder().setDatabase(ByteString.copyFrom(database))).build().toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }

  private static Map.Entry<ByteString, Contents> getHiveTable() {
    byte[] table = new byte[]{0, 1, 2, 3, 4, 5};
    byte[][] partitions = new byte[][]{new byte[]{0, 1, 2, 3, 4, 5}, new byte[]{0, 1, 2, 3, 4, 5}};
    List<ByteString> partitionsBytes = Arrays.stream(partitions).map(ByteString::copyFrom).collect(Collectors.toList());
    Contents contents = ImmutableHiveTable.builder().tableDefinition(table).addPartitions(partitions).id(ID).build();
    ByteString bytes = ObjectTypes.Contents.newBuilder().setId(ID)
      .setHiveTable(ObjectTypes.HiveTable.newBuilder().setTable(ByteString.copyFrom(table))
        .addAllPartition(partitionsBytes)).build().toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }

  private static Map.Entry<ByteString, Contents> getDelta() {
    String path = "foo/bar";
    Contents contents = ImmutableDeltaLakeTable.builder().lastCheckpoint(path).id(ID).build();
    ByteString bytes = ObjectTypes.Contents.newBuilder().setId(ID)
      .setDeltaLakeTable(ObjectTypes.DeltaLakeTable.newBuilder().setLastCheckpoint(path)).build().toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }

  private static Map.Entry<ByteString, Contents> getView() {
    String path = "SELECT * FROM foo.bar,";
    Contents contents = ImmutableSqlView.builder().dialect(SqlView.Dialect.DREMIO).sqlText(path).id(ID).build();
    ByteString bytes = ObjectTypes.Contents.newBuilder().setId(ID)
      .setSqlView(ObjectTypes.SqlView.newBuilder().setSqlText(path).setDialect("DREMIO")).build().toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, contents);
  }
}

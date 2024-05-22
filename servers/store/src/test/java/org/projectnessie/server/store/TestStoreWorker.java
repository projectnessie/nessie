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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import com.google.common.collect.ImmutableMap;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.UDF;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergMetadataPointer;
import org.projectnessie.server.store.proto.ObjectTypes.IcebergRefState;

class TestStoreWorker {

  private static final String ID = "x";
  public static final String CID = "cid";
  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

  @Test
  @SuppressWarnings("deprecation")
  void tableMetadataLocationGlobalNotAvailable() {
    assertThatThrownBy(
            () ->
                worker.valueFromStore(
                    payloadForContent(Content.Type.ICEBERG_TABLE),
                    ObjectTypes.Content.newBuilder()
                        .setId(CID)
                        .setIcebergRefState(
                            IcebergRefState.newBuilder()
                                .setSnapshotId(42)
                                .setSchemaId(43)
                                .setSpecId(44)
                                .setSortOrderId(45))
                        .build()
                        .toByteString(),
                    () -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Iceberg content from reference must have global state, but has none");
  }

  @Test
  @SuppressWarnings("deprecation")
  void tableMetadataLocationGlobal() {
    Content value =
        worker.valueFromStore(
            payloadForContent(Content.Type.ICEBERG_TABLE),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setIcebergRefState(
                    IcebergRefState.newBuilder()
                        .setSnapshotId(42)
                        .setSchemaId(43)
                        .setSpecId(44)
                        .setSortOrderId(45))
                .build()
                .toByteString(),
            () ->
                ObjectTypes.Content.newBuilder()
                    .setId(CID)
                    .setIcebergMetadataPointer(
                        IcebergMetadataPointer.newBuilder()
                            .setMetadataLocation("metadata-location"))
                    .build()
                    .toByteString());
    assertThat(value)
        .isInstanceOf(IcebergTable.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IcebergTable.class))
        .extracting(
            IcebergTable::getMetadataLocation,
            IcebergTable::getSnapshotId,
            IcebergTable::getSchemaId,
            IcebergTable::getSpecId,
            IcebergTable::getSortOrderId)
        .containsExactly("metadata-location", 42L, 43, 44, 45);
  }

  @Test
  void tableMetadataLocationOnRef() {
    Content value =
        worker.valueFromStore(
            payloadForContent(Content.Type.ICEBERG_TABLE),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setIcebergRefState(
                    IcebergRefState.newBuilder()
                        .setSnapshotId(42)
                        .setSchemaId(43)
                        .setSpecId(44)
                        .setSortOrderId(45)
                        .setMetadataLocation("metadata-location"))
                .build()
                .toByteString());
    assertThat(value)
        .isInstanceOf(IcebergTable.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IcebergTable.class))
        .extracting(
            IcebergTable::getMetadataLocation,
            IcebergTable::getSnapshotId,
            IcebergTable::getSchemaId,
            IcebergTable::getSpecId,
            IcebergTable::getSortOrderId)
        .containsExactly("metadata-location", 42L, 43, 44, 45);
  }

  @Test
  @SuppressWarnings("deprecation")
  void viewMetadataLocationGlobalNotAvailable() {
    assertThatThrownBy(
            () ->
                worker.valueFromStore(
                    payloadForContent(Content.Type.ICEBERG_VIEW),
                    ObjectTypes.Content.newBuilder()
                        .setId(CID)
                        .setIcebergViewState(
                            ObjectTypes.IcebergViewState.newBuilder().setVersionId(42))
                        .build()
                        .toByteString(),
                    () -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Iceberg content from reference must have global state, but has none");
  }

  @Test
  @SuppressWarnings("deprecation")
  void viewMetadataLocationGlobal() {
    Content value =
        worker.valueFromStore(
            payloadForContent(Content.Type.ICEBERG_VIEW),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setIcebergViewState(ObjectTypes.IcebergViewState.newBuilder().setVersionId(42))
                .build()
                .toByteString(),
            () ->
                ObjectTypes.Content.newBuilder()
                    .setId(CID)
                    .setIcebergMetadataPointer(
                        IcebergMetadataPointer.newBuilder()
                            .setMetadataLocation("metadata-location"))
                    .build()
                    .toByteString());
    assertThat(value)
        .isInstanceOf(IcebergView.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IcebergView.class))
        .extracting(IcebergView::getMetadataLocation, IcebergView::getVersionId)
        .containsExactly("metadata-location", 42L);
  }

  @SuppressWarnings("deprecation")
  static Stream<Arguments> requiresGlobalStateModelType() {
    return Stream.of(
        Arguments.of(
            UDF.of(CID, "dialect", "sqlText"),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setUdf(ObjectTypes.UDF.newBuilder().setDialect("dialect").setSqlText("sqlText")),
            null,
            Content.Type.UDF),
        //
        Arguments.of(
            UDF.udf(CID, "metadata", "42", "666"),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setUdf(
                    ObjectTypes.UDF
                        .newBuilder()
                        .setMetadataLocation("metadata")
                        .setVersionId("42")
                        .setSignatureId("666")),
            null,
            Content.Type.UDF),
        //
        Arguments.of(
            withId(Namespace.of("foo")),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setNamespace(ObjectTypes.Namespace.newBuilder().addElements("foo")),
            null,
            Content.Type.NAMESPACE),
        //
        Arguments.of(
            IcebergTable.of("metadata", 42, 43, 44, 45, CID),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setIcebergRefState(
                    IcebergRefState.newBuilder()
                        .setSnapshotId(42)
                        .setSchemaId(43)
                        .setSpecId(44)
                        .setSortOrderId(45)
                        .setMetadataLocation("metadata")),
            null,
            Content.Type.ICEBERG_TABLE),
        //
        Arguments.of(
            IcebergView.of(CID, "metadata", 42, 43, "dialect", "sqlText"),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setIcebergViewState(
                    ObjectTypes.IcebergViewState.newBuilder()
                        .setVersionId(42)
                        .setSchemaId(43)
                        .setDialect("dialect")
                        .setSqlText("sqlText")
                        .setMetadataLocation("metadata")),
            null,
            Content.Type.ICEBERG_VIEW),
        //
        Arguments.of(
            ImmutableDeltaLakeTable.builder()
                .id(CID)
                .addCheckpointLocationHistory("check")
                .addMetadataLocationHistory("meta")
                .build(),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setDeltaLakeTable(
                    ObjectTypes.DeltaLakeTable.newBuilder()
                        .addCheckpointLocationHistory("check")
                        .addMetadataLocationHistory("meta")),
            null,
            Content.Type.DELTA_LAKE_TABLE));
  }

  @ParameterizedTest
  @MethodSource("requiresGlobalStateModelType")
  @SuppressWarnings("deprecation")
  void requiresGlobalStateModelType(
      Content content,
      ObjectTypes.Content.Builder onRefBuilder,
      ObjectTypes.Content.Builder globalBuilder,
      Content.Type type) {
    ByteString onRef = onRefBuilder.build().toByteString();
    ByteString global = globalBuilder != null ? globalBuilder.build().toByteString() : null;

    assertThat(onRef)
        .asInstanceOf(InstanceOfAssertFactories.type(ByteString.class))
        .extracting(onRefContent -> worker.getType(payloadForContent(content), onRefContent))
        .isEqualTo(type);
    assertThat(worker.valueFromStore(payloadForContent(content), onRef, () -> global))
        .isEqualTo(content);

    assertThat(content).extracting(worker::toStoreOnReferenceState).isEqualTo(onRef);
  }

  @Test
  void viewMetadataLocationOnRef() {
    Content value =
        worker.valueFromStore(
            payloadForContent(Content.Type.ICEBERG_VIEW),
            ObjectTypes.Content.newBuilder()
                .setId(CID)
                .setIcebergViewState(
                    ObjectTypes.IcebergViewState.newBuilder()
                        .setVersionId(42)
                        .setMetadataLocation("metadata-location"))
                .build()
                .toByteString());
    assertThat(value)
        .isInstanceOf(IcebergView.class)
        .asInstanceOf(InstanceOfAssertFactories.type(IcebergView.class))
        .extracting(IcebergView::getMetadataLocation, IcebergView::getVersionId)
        .containsExactly("metadata-location", 42L);
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testDeserialization(Map.Entry<ByteString, Content> entry) {
    Content actual = worker.valueFromStore(payloadForContent(entry.getValue()), entry.getKey());
    assertThat(actual).isEqualTo(entry.getValue());
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerialization(Map.Entry<ByteString, Content> entry) {
    ByteString actual = worker.toStoreOnReferenceState(entry.getValue());
    assertThat(actual).isEqualTo(entry.getKey());
  }

  @ParameterizedTest
  @MethodSource("provideDeserialization")
  void testSerde(Map.Entry<ByteString, Content> entry) {
    ByteString actualBytes = worker.toStoreOnReferenceState(entry.getValue());
    assertThat(worker.valueFromStore(payloadForContent(entry.getValue()), actualBytes))
        .isEqualTo(entry.getValue());
    Content actualContent =
        worker.valueFromStore(payloadForContent(entry.getValue()), entry.getKey());
    assertThat(worker.toStoreOnReferenceState(actualContent)).isEqualTo(entry.getKey());
  }

  private static Stream<Map.Entry<ByteString, Content>> provideDeserialization() {
    return Stream.of(getDelta(), getNamespace(), getNamespaceWithProperties());
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
    List<String> elements = Arrays.asList("a", "b.c", "d");
    Namespace namespace = withId(Namespace.of(elements));
    ByteString bytes =
        ObjectTypes.Content.newBuilder()
            .setId(namespace.getId())
            .setNamespace(ObjectTypes.Namespace.newBuilder().addAllElements(elements).build())
            .build()
            .toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, namespace);
  }

  private static Map.Entry<ByteString, Content> getNamespaceWithProperties() {
    List<String> elements = Arrays.asList("a", "b.c", "d");
    Map<String, String> properties = ImmutableMap.of("key1", "val1");
    Namespace namespace = withId(Namespace.of(elements, properties));
    ByteString bytes =
        ObjectTypes.Content.newBuilder()
            .setId(namespace.getId())
            .setNamespace(
                ObjectTypes.Namespace.newBuilder()
                    .addAllElements(elements)
                    .putAllProperties(properties)
                    .build())
            .build()
            .toByteString();
    return new AbstractMap.SimpleImmutableEntry<>(bytes, namespace);
  }

  private static Namespace withId(Namespace namespace) {
    return Namespace.builder().from(namespace).id(CID).build();
  }
}

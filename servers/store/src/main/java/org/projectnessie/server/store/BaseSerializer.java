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
package org.projectnessie.server.store;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.ImmutableNamespace;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.store.ContentSerializer;

/**
 * Common content serialization functionality for Iceberg tables+views, Delta Lake tables +
 * namespaces.
 */
abstract class BaseSerializer<C extends Content> implements ContentSerializer<C> {

  @Override
  public ByteString toStoreOnReferenceState(
      C content, Consumer<ContentAttachment> attachmentConsumer) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    toStoreOnRefState(content, builder);
    return builder.build().toByteString();
  }

  @Override
  public boolean requiresGlobalState(byte payload, ByteString onReferenceValue) {
    return false;
  }

  @Override
  public C valueFromStore(
      byte payload,
      ByteString onReferenceValue,
      Supplier<ByteString> globalState,
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever) {
    ObjectTypes.Content content = parse(onReferenceValue);
    return valueFromStore(content, globalState);
  }

  protected abstract C valueFromStore(
      ObjectTypes.Content content, Supplier<ByteString> globalState);

  static ImmutableDeltaLakeTable valueFromStoreDeltaLakeTable(ObjectTypes.Content content) {
    ObjectTypes.DeltaLakeTable deltaLakeTable = content.getDeltaLakeTable();
    ImmutableDeltaLakeTable.Builder builder =
        ImmutableDeltaLakeTable.builder()
            .id(content.getId())
            .addAllMetadataLocationHistory(deltaLakeTable.getMetadataLocationHistoryList())
            .addAllCheckpointLocationHistory(deltaLakeTable.getCheckpointLocationHistoryList());
    if (deltaLakeTable.hasLastCheckpoint()) {
      builder.lastCheckpoint(content.getDeltaLakeTable().getLastCheckpoint());
    }
    return builder.build();
  }

  static ImmutableNamespace valueFromStoreNamespace(ObjectTypes.Content content) {
    ObjectTypes.Namespace namespace = content.getNamespace();
    return ImmutableNamespace.builder()
        .id(content.getId())
        .elements(namespace.getElementsList())
        .putAllProperties(namespace.getPropertiesMap())
        .build();
  }

  static ImmutableIcebergTable valueFromStoreIcebergTable(
      ObjectTypes.Content content, Supplier<String> metadataPointerSupplier) {
    ObjectTypes.IcebergRefState table = content.getIcebergRefState();
    String metadataLocation =
        table.hasMetadataLocation() ? table.getMetadataLocation() : metadataPointerSupplier.get();

    ImmutableIcebergTable.Builder tableBuilder =
        IcebergTable.builder()
            .metadataLocation(metadataLocation)
            .snapshotId(table.getSnapshotId())
            .schemaId(table.getSchemaId())
            .specId(table.getSpecId())
            .sortOrderId(table.getSortOrderId())
            .id(content.getId());

    return tableBuilder.build();
  }

  static ImmutableIcebergView valueFromStoreIcebergView(
      ObjectTypes.Content content, Supplier<String> metadataPointerSupplier) {
    ObjectTypes.IcebergViewState view = content.getIcebergViewState();
    // If the (protobuf) view has the metadataLocation attribute set, use that one, otherwise
    // it's an old representation using global state.
    String metadataLocation =
        view.hasMetadataLocation() ? view.getMetadataLocation() : metadataPointerSupplier.get();

    ImmutableIcebergView.Builder viewBuilder =
        IcebergView.builder()
            .metadataLocation(metadataLocation)
            .versionId(view.getVersionId())
            .schemaId(view.getSchemaId())
            .dialect(view.getDialect())
            .sqlText(view.getSqlText())
            .id(content.getId());

    return viewBuilder.build();
  }

  static IllegalArgumentException noIcebergMetadataPointer() {
    return new IllegalArgumentException(
        "Iceberg content from reference must have global state, but has none");
  }

  protected abstract void toStoreOnRefState(C content, ObjectTypes.Content.Builder builder);

  static ObjectTypes.Content parse(ByteString value) {
    try {
      return ObjectTypes.Content.parseFrom(value);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failure parsing data", e);
    }
  }

  static final class IcebergMetadataPointerSupplier implements Supplier<String> {
    private final Supplier<ByteString> globalState;

    IcebergMetadataPointerSupplier(Supplier<ByteString> globalState) {
      this.globalState = globalState;
    }

    @Override
    public String get() {
      ByteString global = globalState.get();
      if (global == null) {
        throw noIcebergMetadataPointer();
      }
      ObjectTypes.Content globalContent = parse(global);
      if (!globalContent.hasIcebergMetadataPointer()) {
        throw noIcebergMetadataPointer();
      }
      return globalContent.getIcebergMetadataPointer().getMetadataLocation();
    }
  }
}

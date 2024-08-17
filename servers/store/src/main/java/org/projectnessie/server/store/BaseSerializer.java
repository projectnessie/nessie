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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.function.Supplier;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.ImmutableUDF;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.UDF;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.nessie.relocated.protobuf.InvalidProtocolBufferException;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.versioned.store.ContentSerializer;

/**
 * Common content serialization functionality for Iceberg tables+views, Delta Lake tables +
 * namespaces.
 */
@SuppressWarnings("deprecation")
abstract class BaseSerializer<C extends Content> implements ContentSerializer<C> {

  @Override
  public ByteString toStoreOnReferenceState(C content) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    toStoreOnRefState(content, builder);
    return builder.build().toByteString();
  }

  @Override
  public C valueFromStore(ByteString onReferenceValue) {
    ObjectTypes.Content content = parse(onReferenceValue);
    return valueFromStore(content);
  }

  protected abstract C valueFromStore(ObjectTypes.Content content);

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

  static UDF valueFromStoreUDF(ObjectTypes.Content content) {
    ObjectTypes.UDF udf = content.getUdf();
    ImmutableUDF.Builder builder = ImmutableUDF.builder().id(content.getId());
    if (udf.hasDialect()) {
      builder.dialect(udf.getDialect());
    }
    if (udf.hasSqlText()) {
      builder.sqlText(udf.getSqlText());
    }
    if (udf.hasMetadataLocation()) {
      builder.metadataLocation(udf.getMetadataLocation());
    }
    if (udf.hasVersionId()) {
      builder.versionId(udf.getVersionId());
    }
    if (udf.hasSignatureId()) {
      builder.signatureId(udf.getSignatureId());
    }
    return builder.build();
  }

  static Namespace valueFromStoreNamespace(ObjectTypes.Content content) {
    ObjectTypes.Namespace namespace = content.getNamespace();
    return Namespace.builder()
        .id(content.getId())
        .elements(namespace.getElementsList())
        .putAllProperties(namespace.getPropertiesMap())
        .build();
  }

  static IcebergTable valueFromStoreIcebergTable(ObjectTypes.Content content) {
    ObjectTypes.IcebergRefState table = content.getIcebergRefState();
    checkArgument(table.hasMetadataLocation());
    String metadataLocation = table.getMetadataLocation();

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

  static IcebergView valueFromStoreIcebergView(ObjectTypes.Content content) {
    ObjectTypes.IcebergViewState view = content.getIcebergViewState();
    // If the (protobuf) view has the metadataLocation attribute set, use that one, otherwise
    // it's an old representation using global state.
    checkArgument(view.hasMetadataLocation());
    String metadataLocation = view.getMetadataLocation();

    ImmutableIcebergView.Builder viewBuilder =
        IcebergView.builder()
            .metadataLocation(metadataLocation)
            .versionId(view.getVersionId())
            .schemaId(view.getSchemaId())
            .id(content.getId());
    if (view.hasDialect()) {
      viewBuilder.dialect(view.getDialect());
    }
    if (view.hasSqlText()) {
      viewBuilder.sqlText(view.getSqlText());
    }

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

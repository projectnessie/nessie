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

import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.stream.Collectors;
import org.projectnessie.model.IcebergTableMetadata;
import org.projectnessie.model.ImmutableIcebergTableMetadata;
import org.projectnessie.model.ImmutablePartitionField;
import org.projectnessie.model.ImmutablePartitionSpec;
import org.projectnessie.model.ImmutableSnapshot;
import org.projectnessie.model.ImmutableSortField;
import org.projectnessie.model.ImmutableSortOrder;
import org.projectnessie.model.PartitionSpec;
import org.projectnessie.model.Snapshot;
import org.projectnessie.model.SortField.NullOrder;
import org.projectnessie.model.SortField.SortDirection;
import org.projectnessie.model.SortOrder;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.store.ObjectTypes.IcebergMetadataPointer;
import org.projectnessie.store.ObjectTypes.IcebergProperties;
import org.projectnessie.store.ObjectTypes.IcebergRefState;
import org.projectnessie.store.ObjectTypes.IcebergSnapshot;
import org.projectnessie.store.ObjectTypes.IcebergTableSchema;
import org.projectnessie.store.ObjectTypes.IcebergTableSortOrder;
import org.projectnessie.store.ObjectTypes.IcebergTableSpec;
import org.projectnessie.store.ObjectTypes.PartitionField;
import org.projectnessie.store.ObjectTypes.SortField;

public final class IcebergMetadataSerde {

  private IcebergMetadataSerde() {}

  static Map.Entry<String, ByteString> snapshotToProto(IcebergTableMetadata metadata) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder();
    IcebergSnapshot.Builder objBuilder =
        IcebergSnapshot.newBuilder()
            .setSnapshotId(metadata.getSnapshotId())
            .setSequenceNumber(metadata.getSnapshot().getSequenceNumber())
            .setParentSnapshotId(metadata.getSnapshot().getParentSnapshotId())
            .setTimestampMillis(metadata.getSnapshot().getTimestampMillis())
            .setSchemaId(metadata.getSnapshot().getSchemaId())
            .setOperation(metadata.getSnapshot().getOperation())
            .setManifestList(metadata.getSnapshot().getManifestList())
            .addAllSummary(
                metadata.getSnapshot().getSummary().entrySet().stream()
                    .map(
                        kv ->
                            IcebergProperties.newBuilder()
                                .setKey(kv.getKey())
                                .setValue(kv.getValue())
                                .build())
                    .collect(Collectors.toList()));
    ByteString obj = objBuilder.build().toByteString();
    String key = toKey(obj);
    builder.setIcebergSnapshot(objBuilder).setId(key);
    return new SimpleImmutableEntry<>(key, builder.build().toByteString());
  }

  private static String toKey(ByteString bytes) {
    return Hashing.sha256().newHasher().putBytes(bytes.toByteArray()).hash().toString();
  }

  static Map.Entry<String, ByteString> specToProto(IcebergTableMetadata metadata) {

    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder();
    IcebergTableSpec.Builder objBuilder =
        IcebergTableSpec.newBuilder()
            .setSpecId(metadata.getSpecId())
            .setLastAssignedFieldId(metadata.getSpec().getLastAssignedFieldId())
            .setSchema(metadata.getSpec().getSchema())
            .addAllFields(
                metadata.getSpec().getFields().stream()
                    .map(
                        field ->
                            PartitionField.newBuilder()
                                .setSourceId(field.getSourceId())
                                .setFieldId(field.getFieldId())
                                .setName(field.getName())
                                .setTransform(field.getTransform())
                                .build())
                    .collect(Collectors.toList()));
    ByteString obj = objBuilder.build().toByteString();
    String key = toKey(obj);
    builder.setIcebergSpec(objBuilder).setId(key);
    return new SimpleImmutableEntry<>(key, builder.build().toByteString());
  }

  static Map.Entry<String, ByteString> schemaToProto(IcebergTableMetadata metadata) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder();
    IcebergTableSchema.Builder objBuilder =
        IcebergTableSchema.newBuilder()
            .setSchemaId(metadata.getSchemaId())
            .setSchema(metadata.getSchema());
    ByteString obj = objBuilder.build().toByteString();
    String key = toKey(obj);
    builder.setIcebergSchema(objBuilder).setId(key);
    return new SimpleImmutableEntry<>(key, builder.build().toByteString());
  }

  static Map.Entry<String, ByteString> metadataToProto(IcebergTableMetadata metadata) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder();
    ObjectTypes.IcebergTableMetadata.Builder objBuilder =
        ObjectTypes.IcebergTableMetadata.newBuilder()
            .setFormatVersion(metadata.getFormatVersion())
            .setLastColumnId(metadata.getLastColumnId())
            .setLastSequenceNumber(metadata.getLastSequenceNumber())
            .setLastUpdatedMillis(metadata.getLastUpdatedMillis())
            .setUuid(metadata.getUuid())
            .setLocation(metadata.getLocation())
            .setLastAssignedPartitionId(metadata.getLastAssignedPartitionId())
            .addAllProperties(
                metadata.getProperties().entrySet().stream()
                    .map(
                        kv ->
                            IcebergProperties.newBuilder()
                                .setKey(kv.getKey())
                                .setValue(kv.getValue())
                                .build())
                    .collect(Collectors.toList()));
    ByteString obj = objBuilder.build().toByteString();
    String key = toKey(obj);
    builder.setIcebergMetadata(objBuilder).setId(key);
    return new SimpleImmutableEntry<>(key, builder.build().toByteString());
  }

  static Map.Entry<String, ByteString> sortOrderToProto(IcebergTableMetadata metadata) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder();
    SortOrder sortOrder = metadata.getSortOrder();
    IcebergTableSortOrder.Builder objBuilder =
        IcebergTableSortOrder.newBuilder()
            .setSortOrderId(sortOrder.getOrderId())
            .setSchema(sortOrder.getSchema())
            .addAllFields(
                sortOrder.getFields().stream()
                    .map(
                        field ->
                            SortField.newBuilder()
                                .setSourceId(field.getSourceId())
                                .setSortDirection(field.getDirection().ordinal())
                                .setNullOrder(field.getNullOrder().ordinal())
                                .setTransform(field.getTransform())
                                .build())
                    .collect(Collectors.toList()));
    ByteString obj = objBuilder.build().toByteString();
    String key = toKey(obj);
    builder.setIcebergSortOrder(objBuilder).setId(key);
    return new SimpleImmutableEntry<>(key, builder.build().toByteString());
  }

  public static IcebergTableMetadata protoToMetadata(
      IcebergTableSchema schemaObject,
      IcebergSnapshot snapshotObject,
      IcebergTableSpec specObject,
      IcebergTableSortOrder sortObject,
      ObjectTypes.IcebergTableMetadata metadataObject,
      IcebergMetadataPointer icebergMetadataPointer,
      IcebergRefState refState) {
    return ImmutableIcebergTableMetadata.builder()
        .metadataLocation(icebergMetadataPointer.getMetadataLocation())
        .formatVersion(metadataObject.getFormatVersion())
        .uuid(metadataObject.getUuid())
        .location(metadataObject.getLocation())
        .lastSequenceNumber(metadataObject.getLastSequenceNumber())
        .lastUpdatedMillis(metadataObject.getLastUpdatedMillis())
        .lastColumnId(metadataObject.getLastColumnId())
        .schema(getSchema(schemaObject))
        .spec(getSpec(specObject))
        .sortOrder(getSortOrder(sortObject))
        .snapshot(getSnapshot(snapshotObject))
        .putAllProperties(
            metadataObject.getPropertiesList().stream()
                .collect(Collectors.toMap(IcebergProperties::getKey, IcebergProperties::getValue)))
        .lastAssignedPartitionId(metadataObject.getLastAssignedPartitionId())
        .snapshotId(refState.getSnapshotId())
        .schemaId(refState.getSchemaId())
        .specId(refState.getSpecId())
        .sortOrderId(refState.getSortOrderId())
        .build();
  }

  private static Snapshot getSnapshot(IcebergSnapshot snapshotObject) {
    return ImmutableSnapshot.builder()
        .sequenceNumber(snapshotObject.getSequenceNumber())
        .snapshotId(snapshotObject.getSnapshotId())
        .parentSnapshotId(snapshotObject.getParentSnapshotId())
        .timestampMillis(snapshotObject.getTimestampMillis())
        .operation(snapshotObject.getOperation())
        .putAllSummary(
            snapshotObject.getSummaryList().stream()
                .collect(Collectors.toMap(IcebergProperties::getKey, IcebergProperties::getValue)))
        .schemaId(snapshotObject.getSchemaId())
        .manifestList(snapshotObject.getManifestList())
        .build();
  }

  private static SortOrder getSortOrder(IcebergTableSortOrder sortObject) {
    return ImmutableSortOrder.builder()
        .schema(sortObject.getSchema())
        .orderId(sortObject.getSortOrderId())
        .addAllFields(
            sortObject.getFieldsList().stream()
                .map(
                    x ->
                        ImmutableSortField.builder()
                            .nullOrder(NullOrder.values()[x.getNullOrder()])
                            .transform(x.getTransform())
                            .direction(SortDirection.values()[x.getSortDirection()])
                            .sourceId(x.getSourceId())
                            .build())
                .collect(Collectors.toList()))
        .build();
  }

  private static PartitionSpec getSpec(IcebergTableSpec specObject) {
    return ImmutablePartitionSpec.builder()
        .schema(specObject.getSchema())
        .addAllFields(
            specObject.getFieldsList().stream()
                .map(
                    x ->
                        ImmutablePartitionField.builder()
                            .transform(x.getTransform())
                            .name(x.getName())
                            .fieldId(x.getFieldId())
                            .sourceId(x.getSourceId())
                            .build())
                .collect(Collectors.toList()))
        .specId(specObject.getSpecId())
        .lastAssignedFieldId(specObject.getLastAssignedFieldId())
        .build();
  }

  private static String getSchema(IcebergTableSchema schemaObject) {
    return schemaObject.getSchema();
  }
}

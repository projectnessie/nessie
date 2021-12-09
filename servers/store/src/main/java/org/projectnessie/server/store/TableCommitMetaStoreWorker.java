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

import static org.projectnessie.server.store.IcebergMetadataSerde.metadataToProto;
import static org.projectnessie.server.store.IcebergMetadataSerde.schemaToProto;
import static org.projectnessie.server.store.IcebergMetadataSerde.snapshotToProto;
import static org.projectnessie.server.store.IcebergMetadataSerde.sortOrderToProto;
import static org.projectnessie.server.store.IcebergMetadataSerde.specToProto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergTableMetadata;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDeltaLakeTable.Builder;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.store.ObjectTypes.Content.ObjectTypeCase;
import org.projectnessie.store.ObjectTypes.IcebergRefState;
import org.projectnessie.store.ObjectTypes.IcebergSnapshot;
import org.projectnessie.store.ObjectTypes.IcebergTableSchema;
import org.projectnessie.store.ObjectTypes.IcebergTableSortOrder;
import org.projectnessie.store.ObjectTypes.IcebergTableSpec;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;

public class TableCommitMetaStoreWorker implements StoreWorker<Content, CommitMeta, Content.Type> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public ByteString toStoreOnReferenceState(Content content) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    if (content instanceof IcebergTable) {
      IcebergTable state = (IcebergTable) content;
      ObjectTypes.IcebergRefState.Builder stateBuilder =
          ObjectTypes.IcebergRefState.newBuilder()
              .setSnapshotId(state.getSnapshotId())
              .setSchemaId(state.getSchemaId())
              .setSpecId(state.getSpecId())
              .setSortOrderId(state.getSortOrderId());
      builder.setIcebergRefState(stateBuilder);
    } else if (content instanceof IcebergTableMetadata) {
      IcebergTableMetadata state = (IcebergTableMetadata) content;
      ObjectTypes.IcebergRefState.Builder stateBuilder =
          ObjectTypes.IcebergRefState.newBuilder()
              .setSnapshotId(state.getSnapshotId())
              .setSnapshotHash(snapshotToProto(state).getKey())
              .setSchemaId(state.getSchemaId())
              .setSchemaHash(schemaToProto(state).getKey())
              .setSpecId(state.getSpecId())
              .setSpecHash(specToProto(state).getKey())
              .setSortOrderId(state.getSortOrderId())
              .setSortOrderHash(sortOrderToProto(state).getKey())
              .setMetadataHash(metadataToProto(state).getKey());
      builder.setIcebergRefState(stateBuilder);
    } else if (content instanceof DeltaLakeTable) {
      ObjectTypes.DeltaLakeTable.Builder table =
          ObjectTypes.DeltaLakeTable.newBuilder()
              .addAllMetadataLocationHistory(
                  ((DeltaLakeTable) content).getMetadataLocationHistory())
              .addAllCheckpointLocationHistory(
                  ((DeltaLakeTable) content).getCheckpointLocationHistory());
      String lastCheckpoint = ((DeltaLakeTable) content).getLastCheckpoint();
      if (lastCheckpoint != null) {
        table.setLastCheckpoint(lastCheckpoint);
      }
      builder.setDeltaLakeTable(table);

    } else if (content instanceof SqlView) {
      SqlView view = (SqlView) content;
      builder.setSqlView(
          ObjectTypes.SqlView.newBuilder()
              .setDialect(view.getDialect().name())
              .setSqlText(view.getSqlText()));
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }

    return builder.build().toByteString();
  }

  @Override
  public ByteString toStoreGlobalState(Content content) {
    ObjectTypes.Content.Builder builder = ObjectTypes.Content.newBuilder().setId(content.getId());
    if (content instanceof IcebergTable) {
      IcebergTable state = (IcebergTable) content;
      ObjectTypes.IcebergMetadataPointer.Builder stateBuilder =
          ObjectTypes.IcebergMetadataPointer.newBuilder()
              .setMetadataLocation(state.getMetadataLocation());
      builder.setIcebergMetadataPointer(stateBuilder);
    } else if (content instanceof IcebergTableMetadata) {
      IcebergTableMetadata state = (IcebergTableMetadata) content;
      ObjectTypes.IcebergMetadataPointer.Builder stateBuilder =
          ObjectTypes.IcebergMetadataPointer.newBuilder()
              .setMetadataLocation(state.getMetadataLocation());
      builder.setIcebergMetadataPointer(stateBuilder);
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }

    return builder.build().toByteString();
  }

  @Override
  public Map<String, ByteString> toStoreAuxiliaryGlobalState(Content content) {
    if (content instanceof IcebergTableMetadata) {
      IcebergTableMetadata metadata = (IcebergTableMetadata) content;
      ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
      builder.put(snapshotToProto(metadata));
      builder.put(specToProto(metadata));
      builder.put(schemaToProto(metadata));
      builder.put(metadataToProto(metadata));
      builder.put(sortOrderToProto(metadata));
      return builder.build();
    }
    return ImmutableMap.of();
  }

  @Override
  public List<String> multiValueFromStoreIds(
      ByteString onReferenceValue, Optional<ByteString> globalState) {
    ObjectTypes.Content content = parse(onReferenceValue);
    Optional<ObjectTypes.Content> globalContent =
        globalState.map(TableCommitMetaStoreWorker::parse);
    if (content.getObjectTypeCase() == ObjectTypeCase.ICEBERG_REF_STATE) {
      ObjectTypes.Content global =
          globalContent.orElseThrow(TableCommitMetaStoreWorker::noIcebergMetadataPointer);
      if (!global.hasIcebergMetadataPointer()) {
        throw noIcebergMetadataPointer();
      }
      IcebergRefState refState = content.getIcebergRefState();
      return ImmutableList.of(
          refState.getSchemaHash(),
          refState.getSnapshotHash(),
          refState.getSpecHash(),
          refState.getSortOrderHash(),
          refState.getMetadataHash());
    }
    return new ArrayList<>();
  }

  @Override
  public Content multiValueFromStore(ByteString onReferenceValue, List<ByteString> globalState) {
    ObjectTypes.Content content = parse(onReferenceValue);
    Map<String, ObjectTypes.Content> globalContent =
        globalState.stream()
            .map(TableCommitMetaStoreWorker::parse)
            .collect(Collectors.toMap(ObjectTypes.Content::getId, Function.identity()));
    if (content.getObjectTypeCase() == ObjectTypeCase.ICEBERG_REF_STATE) {
      IcebergRefState refState = content.getIcebergRefState();
      IcebergTableSchema schemaObject =
          globalContent.remove(refState.getSchemaHash()).getIcebergSchema();
      IcebergSnapshot snapshotObject =
          globalContent.remove(refState.getSnapshotHash()).getIcebergSnapshot();
      IcebergTableSpec specObject = globalContent.remove(refState.getSpecHash()).getIcebergSpec();
      IcebergTableSortOrder sortObject =
          globalContent.remove(refState.getSortOrderHash()).getIcebergSortOrder();
      ObjectTypes.IcebergTableMetadata metadataObject =
          globalContent.remove(refState.getMetadataHash()).getIcebergMetadata();

      if (globalContent.size() != 1) {
        throw noIcebergMetadataPointer();
      }
      ObjectTypes.Content global = globalContent.values().iterator().next();
      if (!global.hasIcebergMetadataPointer()) {
        throw noIcebergMetadataPointer();
      }
      return IcebergMetadataSerde.protoToMetadata(
          schemaObject,
          snapshotObject,
          specObject,
          sortObject,
          metadataObject,
          global.getIcebergMetadataPointer(),
          refState);
    }
    throw new IllegalArgumentException("Unknown type " + content.getObjectTypeCase());
  }

  @Override
  public Content valueFromStore(ByteString onReferenceValue, Optional<ByteString> globalState) {
    ObjectTypes.Content content = parse(onReferenceValue);
    Optional<ObjectTypes.Content> globalContent =
        globalState.map(TableCommitMetaStoreWorker::parse);
    switch (content.getObjectTypeCase()) {
      case DELTA_LAKE_TABLE:
        Builder builder =
            ImmutableDeltaLakeTable.builder()
                .id(content.getId())
                .addAllMetadataLocationHistory(
                    content.getDeltaLakeTable().getMetadataLocationHistoryList())
                .addAllCheckpointLocationHistory(
                    content.getDeltaLakeTable().getCheckpointLocationHistoryList());
        if (content.getDeltaLakeTable().getLastCheckpoint() != null) {
          builder.lastCheckpoint(content.getDeltaLakeTable().getLastCheckpoint());
        }
        return builder.build();

      case ICEBERG_REF_STATE:
        ObjectTypes.Content global =
            globalContent.orElseThrow(TableCommitMetaStoreWorker::noIcebergMetadataPointer);
        if (!global.hasIcebergMetadataPointer()) {
          throw noIcebergMetadataPointer();
        }
        return ImmutableIcebergTable.builder()
            .metadataLocation(global.getIcebergMetadataPointer().getMetadataLocation())
            .snapshotId(content.getIcebergRefState().getSnapshotId())
            .schemaId(content.getIcebergRefState().getSchemaId())
            .specId(content.getIcebergRefState().getSpecId())
            .sortOrderId(content.getIcebergRefState().getSortOrderId())
            .id(content.getId())
            .build();

      case SQL_VIEW:
        ObjectTypes.SqlView view = content.getSqlView();
        return ImmutableSqlView.builder()
            .dialect(Dialect.valueOf(view.getDialect()))
            .sqlText(view.getSqlText())
            .id(content.getId())
            .build();

      case OBJECTTYPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Unknown type " + content.getObjectTypeCase());
    }
  }

  private static IllegalArgumentException noIcebergMetadataPointer() {
    return new IllegalArgumentException(
        "Iceberg content from reference must have global state, but has none");
  }

  @Override
  public String getId(Content content) {
    return content.getId();
  }

  @Override
  public Byte getPayload(Content content) {
    if (content instanceof IcebergTable) {
      return (byte) Content.Type.ICEBERG_TABLE.ordinal();
    } else if (content instanceof DeltaLakeTable) {
      return (byte) Content.Type.DELTA_LAKE_TABLE.ordinal();
    } else if (content instanceof SqlView) {
      return (byte) Content.Type.VIEW.ordinal();
    } else if (content instanceof IcebergTableMetadata) {
      return (byte) Type.ICEBERG_METADATA.ordinal();
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }
  }

  @Override
  public Type getType(Byte payload) {
    if (payload == null || payload > Content.Type.values().length || payload < 0) {
      throw new IllegalArgumentException(
          String.format("Cannot create type from payload. Payload %d does not exist", payload));
    }
    return Content.Type.values()[payload];
  }

  @Override
  public boolean requiresGlobalState(Content content) {
    return content instanceof IcebergTable || content instanceof IcebergTableMetadata;
  }

  @Override
  public boolean requiresGlobalState(Type type) {
    return type == Type.ICEBERG_TABLE;
  }

  private static ObjectTypes.Content parse(ByteString onReferenceValue) {
    try {
      return ObjectTypes.Content.parseFrom(onReferenceValue);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failure parsing data", e);
    }
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  private static class MetadataSerializer implements Serializer<CommitMeta> {
    @Override
    public ByteString toBytes(CommitMeta value) {
      try {
        return ByteString.copyFrom(MAPPER.writeValueAsBytes(value));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(String.format("Couldn't serialize commit meta %s", value), e);
      }
    }

    @Override
    public CommitMeta fromBytes(ByteString bytes) {
      try {
        return MAPPER.readValue(bytes.toByteArray(), CommitMeta.class);
      } catch (IOException e) {
        return ImmutableCommitMeta.builder()
            .message("unknown")
            .committer("unknown")
            .hash("unknown")
            .build();
      }
    }
  }
}

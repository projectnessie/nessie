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
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Optional;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDeltaLakeTable.Builder;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.store.ObjectTypes;
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
    } else if (content instanceof IcebergView) {
      IcebergView view = (IcebergView) content;
      builder.setIcebergViewState(
          ObjectTypes.IcebergViewState.newBuilder()
              .setVersionId(view.getVersionId())
              .setSchemaId(view.getSchemaId())
              .setDialect(view.getDialect())
              .setSqlText(view.getSqlText()));
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
    } else if (content instanceof IcebergView) {
      IcebergView state = (IcebergView) content;
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

      case ICEBERG_VIEW_STATE:
        ObjectTypes.Content globalView =
            globalContent.orElseThrow(TableCommitMetaStoreWorker::noIcebergMetadataPointer);
        if (!globalView.hasIcebergMetadataPointer()) {
          throw noIcebergMetadataPointer();
        }
        ObjectTypes.IcebergViewState view = content.getIcebergViewState();
        return ImmutableIcebergView.builder()
            .metadataLocation(globalView.getIcebergMetadataPointer().getMetadataLocation())
            .versionId(view.getVersionId())
            .schemaId(view.getSchemaId())
            .dialect(view.getDialect())
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
    } else if (content instanceof IcebergView) {
      return (byte) Content.Type.ICEBERG_VIEW.ordinal();
    } else {
      throw new IllegalArgumentException("Unknown type " + content);
    }
  }

  @Override
  public Type getType(Content content) {
    if (content instanceof IcebergTable) {
      return Type.ICEBERG_TABLE;
    } else if (content instanceof IcebergView) {
      return Type.ICEBERG_VIEW;
    } else if (content instanceof DeltaLakeTable) {
      return Type.DELTA_LAKE_TABLE;
    }
    throw new IllegalArgumentException("Unknown type " + content);
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
  public Type getType(ByteString onRefContent) {
    ObjectTypes.Content parsed = parse(onRefContent);

    if (parsed.hasIcebergViewState()) {
      return Type.ICEBERG_TABLE;
    }
    if (parsed.hasIcebergRefState()) {
      return Type.ICEBERG_VIEW;
    }

    if (parsed.hasDeltaLakeTable()) {
      return Type.DELTA_LAKE_TABLE;
    }

    throw new IllegalArgumentException("Unsupported on-ref content " + parsed);
  }

  @Override
  public boolean requiresGlobalState(Enum<Type> type) {
    return type == Type.ICEBERG_TABLE || type == Type.ICEBERG_VIEW;
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

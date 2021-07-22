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
import com.google.protobuf.UnsafeByteOperations;
import java.io.IOException;
import java.util.stream.Collectors;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.GlobalContents;
import org.projectnessie.model.HiveDatabase;
import org.projectnessie.model.HiveTable;
import org.projectnessie.model.IcebergSnapshot;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDeltaLakeTable.Builder;
import org.projectnessie.model.ImmutableHiveDatabase;
import org.projectnessie.model.ImmutableHiveTable;
import org.projectnessie.model.ImmutableIcebergSnapshot;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.StoreWorker;

public class TableCommitMetaStoreWorker
    implements StoreWorker<Contents, GlobalContents, CommitMeta, Contents.Type> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SerializerWithPayload<Contents, Contents.Type> tableSerializer =
      new TableValueSerializer();
  private final Serializer<GlobalContents> stateSerializer = new GlobalStateSerializer();
  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public SerializerWithPayload<Contents, Contents.Type> getValueSerializer() {
    return tableSerializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  @Override
  public Serializer<GlobalContents> getStateSerializer() {
    return stateSerializer;
  }

  @Override
  public Contents mergeGlobalState(Contents contents, GlobalContents globalContents) {
    return contents.withGlobalState(globalContents);
  }

  @Override
  public GlobalContents extractGlobalState(Contents contents) {
    return contents.extractGlobalState();
  }

  private static class GlobalStateSerializer implements Serializer<GlobalContents> {
    @Override
    public ByteString toBytes(GlobalContents value) {
      ObjectTypes.GLobalContents.Builder builder =
          ObjectTypes.GLobalContents.newBuilder().setId(value.getId());
      if (value instanceof IcebergTable) {
        IcebergTable snapshot = (IcebergTable) value;
        builder.setIcebergTable(
            ObjectTypes.IcebergTable.newBuilder()
                .setMetadataLocation(snapshot.getMetadataLocation()));
      } else {
        throw new IllegalArgumentException("Unknown type" + value);
      }

      return builder.build().toByteString();
    }

    @Override
    public GlobalContents fromBytes(ByteString bytes) {
      ObjectTypes.GLobalContents contents;
      try {
        contents = ObjectTypes.GLobalContents.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Failure parsing data", e);
      }
      switch (contents.getObjectTypeCase()) {
        case ICEBERG_TABLE:
          return ImmutableIcebergTable.builder()
              .metadataLocation(contents.getIcebergTable().getMetadataLocation())
              .id(contents.getId())
              .build();
        case OBJECTTYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unknown type" + contents.getObjectTypeCase());
      }
    }
  }

  private static class TableValueSerializer
      implements SerializerWithPayload<Contents, Contents.Type> {
    @Override
    public ByteString toBytes(Contents value) {
      ObjectTypes.Contents.Builder builder = ObjectTypes.Contents.newBuilder().setId(value.getId());
      if (value instanceof IcebergSnapshot) {
        IcebergSnapshot snapshot = (IcebergSnapshot) value;
        ObjectTypes.IcebergSnapshot.Builder snapshotBuilder =
            ObjectTypes.IcebergSnapshot.newBuilder()
                .setMetadataLocation(snapshot.getMetadataLocation());
        if (snapshot.getCurrentSnapshotId() != null) {
          snapshotBuilder.setCurrentSnapshotId(snapshot.getCurrentSnapshotId());
        }
        builder.setIcebergSnapshot(snapshotBuilder);

      } else if (value instanceof DeltaLakeTable) {

        ObjectTypes.DeltaLakeTable.Builder table =
            ObjectTypes.DeltaLakeTable.newBuilder()
                .addAllMetadataLocationHistory(
                    ((DeltaLakeTable) value).getMetadataLocationHistory())
                .addAllCheckpointLocationHistory(
                    ((DeltaLakeTable) value).getCheckpointLocationHistory());
        String lastCheckpoint = ((DeltaLakeTable) value).getLastCheckpoint();
        if (lastCheckpoint != null) {
          table.setLastCheckpoint(lastCheckpoint);
        }
        builder.setDeltaLakeTable(table);

      } else if (value instanceof HiveTable) {
        HiveTable ht = (HiveTable) value;
        builder.setHiveTable(
            ObjectTypes.HiveTable.newBuilder()
                .setTable(UnsafeByteOperations.unsafeWrap(ht.getTableDefinition()))
                .addAllPartition(
                    ht.getPartitions().stream()
                        .map(UnsafeByteOperations::unsafeWrap)
                        .collect(Collectors.toList())));

      } else if (value instanceof HiveDatabase) {
        builder.setHiveDatabase(
            ObjectTypes.HiveDatabase.newBuilder()
                .setDatabase(
                    UnsafeByteOperations.unsafeWrap(
                        ((HiveDatabase) value).getDatabaseDefinition())));
      } else if (value instanceof SqlView) {
        SqlView view = (SqlView) value;
        builder.setSqlView(
            ObjectTypes.SqlView.newBuilder()
                .setDialect(view.getDialect().name())
                .setSqlText(view.getSqlText()));
      } else {
        throw new IllegalArgumentException("Unknown type" + value);
      }

      return builder.build().toByteString();
    }

    @Override
    public Contents fromBytes(ByteString bytes) {
      ObjectTypes.Contents contents;
      try {
        contents = ObjectTypes.Contents.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Failure parsing data", e);
      }
      switch (contents.getObjectTypeCase()) {
        case DELTA_LAKE_TABLE:
          Builder builder =
              ImmutableDeltaLakeTable.builder()
                  .id(contents.getId())
                  .addAllMetadataLocationHistory(
                      contents.getDeltaLakeTable().getMetadataLocationHistoryList())
                  .addAllCheckpointLocationHistory(
                      contents.getDeltaLakeTable().getCheckpointLocationHistoryList());
          if (contents.getDeltaLakeTable().getLastCheckpoint() != null) {
            builder.lastCheckpoint(contents.getDeltaLakeTable().getLastCheckpoint());
          }
          return builder.build();

        case HIVE_DATABASE:
          return ImmutableHiveDatabase.builder()
              .id(contents.getId())
              .databaseDefinition(contents.getHiveDatabase().getDatabase().toByteArray())
              .build();

        case HIVE_TABLE:
          return ImmutableHiveTable.builder()
              .id(contents.getId())
              .addAllPartitions(
                  contents.getHiveTable().getPartitionList().stream()
                      .map(ByteString::toByteArray)
                      .collect(Collectors.toList()))
              .tableDefinition(contents.getHiveTable().getTable().toByteArray())
              .build();

        case ICEBERG_SNAPSHOT:
          return ImmutableIcebergSnapshot.builder()
              .metadataLocation(contents.getIcebergSnapshot().getMetadataLocation())
              .currentSnapshotId(contents.getIcebergSnapshot().getCurrentSnapshotId())
              .id(contents.getId())
              .build();

        case SQL_VIEW:
          ObjectTypes.SqlView view = contents.getSqlView();
          return ImmutableSqlView.builder()
              .dialect(Dialect.valueOf(view.getDialect()))
              .sqlText(view.getSqlText())
              .id(contents.getId())
              .build();

        case OBJECTTYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unknown type" + contents.getObjectTypeCase());
      }
    }

    @Override
    public Byte getPayload(Contents value) {
      if (value instanceof IcebergSnapshot) {
        return (byte) Contents.Type.ICEBERG_SNAPSHOT.ordinal();
      } else if (value instanceof DeltaLakeTable) {
        return (byte) Contents.Type.DELTA_LAKE_TABLE.ordinal();
      } else if (value instanceof HiveTable) {
        return (byte) Contents.Type.HIVE_TABLE.ordinal();
      } else if (value instanceof HiveDatabase) {
        return (byte) Contents.Type.HIVE_DATABASE.ordinal();
      } else if (value instanceof SqlView) {
        return (byte) Contents.Type.VIEW.ordinal();
      } else {
        throw new IllegalArgumentException("Unknown type" + value);
      }
    }

    @Override
    public Contents.Type getType(Byte payload) {
      if (payload == null || payload > Contents.Type.values().length || payload < 0) {
        throw new IllegalArgumentException(
            String.format("Cannot create type from payload. Payload %d does not exist", payload));
      }
      return Contents.Type.values()[payload];
    }
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

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

import java.io.IOException;
import java.util.stream.Collectors;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.DremioDialect;
import org.projectnessie.model.HiveDatabase;
import org.projectnessie.model.HiveDialect;
import org.projectnessie.model.HiveTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDeltaLakeTable.Builder;
import org.projectnessie.model.ImmutableDremioDialect;
import org.projectnessie.model.ImmutableHiveDatabase;
import org.projectnessie.model.ImmutableHiveDialect;
import org.projectnessie.model.ImmutableHiveTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableSparkDialect;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.ImmutableTrinoDialect;
import org.projectnessie.model.SparkDialect;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.Dialect;
import org.projectnessie.model.TrinoDialect;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.SerializerWithPayload;
import org.projectnessie.versioned.StoreWorker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;

public class TableCommitMetaStoreWorker implements StoreWorker<Contents, CommitMeta, Contents.Type> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final SerializerWithPayload<Contents, Contents.Type> tableSerializer = new TableValueSerializer();
  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public SerializerWithPayload<Contents, Contents.Type> getValueSerializer() {
    return tableSerializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  private static class TableValueSerializer implements SerializerWithPayload<Contents, Contents.Type> {
    @Override
    public ByteString toBytes(Contents value) {
      ObjectTypes.Contents.Builder builder = ObjectTypes.Contents.newBuilder().setId(value.getId());
      if (value instanceof IcebergTable) {
        builder.setIcebergTable(
            ObjectTypes.IcebergTable.newBuilder().setMetadataLocation(((IcebergTable) value).getMetadataLocation()));

      } else if (value instanceof DeltaLakeTable) {

        ObjectTypes.DeltaLakeTable.Builder table = ObjectTypes.DeltaLakeTable.newBuilder()
            .addAllMetadataLocationHistory(((DeltaLakeTable) value).getMetadataLocationHistory())
            .addAllCheckpointLocationHistory(((DeltaLakeTable) value).getCheckpointLocationHistory());
        String lastCheckpoint = ((DeltaLakeTable) value).getLastCheckpoint();
        if (lastCheckpoint != null) {
          table.setLastCheckpoint(lastCheckpoint);
        }
        builder.setDeltaLakeTable(table);

      } else if (value instanceof HiveTable) {
        HiveTable ht = (HiveTable) value;
        builder.setHiveTable(ObjectTypes.HiveTable.newBuilder()
            .setTable(UnsafeByteOperations.unsafeWrap(ht.getTableDefinition())).addAllPartition(
                ht.getPartitions().stream().map(UnsafeByteOperations::unsafeWrap).collect(Collectors.toList())));

      } else if (value instanceof HiveDatabase) {
        builder.setHiveDatabase(ObjectTypes.HiveDatabase.newBuilder()
            .setDatabase(UnsafeByteOperations.unsafeWrap(((HiveDatabase) value).getDatabaseDefinition())));
      } else if (value instanceof SqlView) {
        SqlView view = (SqlView) value;
        builder.setSqlView(ObjectTypes.SqlView.newBuilder().setDialect(convertFromDialect(view.getDialect()).build()).setSqlText(view.getSqlText()));
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
          Builder builder = ImmutableDeltaLakeTable.builder().id(contents.getId())
              .addAllMetadataLocationHistory(contents.getDeltaLakeTable().getMetadataLocationHistoryList())
              .addAllCheckpointLocationHistory(contents.getDeltaLakeTable().getCheckpointLocationHistoryList());
          if (contents.getDeltaLakeTable().getLastCheckpoint() != null) {
            builder.lastCheckpoint(contents.getDeltaLakeTable().getLastCheckpoint());
          }
          return builder.build();

        case HIVE_DATABASE:
          return ImmutableHiveDatabase.builder().id(contents.getId())
              .databaseDefinition(contents.getHiveDatabase().getDatabase().toByteArray()).build();

        case HIVE_TABLE:
          return ImmutableHiveTable.builder().id(contents.getId())
              .addAllPartitions(contents.getHiveTable().getPartitionList().stream().map(ByteString::toByteArray)
                  .collect(Collectors.toList()))
              .tableDefinition(contents.getHiveTable().getTable().toByteArray()).build();

        case ICEBERG_TABLE:
          return ImmutableIcebergTable.builder().metadataLocation(contents.getIcebergTable().getMetadataLocation())
              .id(contents.getId()).build();

        case SQL_VIEW:
          ObjectTypes.SqlView view = contents.getSqlView();
          return ImmutableSqlView.builder().dialect(convertToDialect(view.getDialect())).sqlText(view.getSqlText())
            .id(contents.getId()).build();

        case OBJECTTYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unknown type" + contents.getObjectTypeCase());

      }
    }

    private ObjectTypes.Dialect.Builder convertFromDialect(Dialect value) {
      ObjectTypes.Dialect.Builder builder = ObjectTypes.Dialect.newBuilder();
      if (value instanceof DremioDialect) {
        builder.setDremioDialect(ObjectTypes.DremioDialect.newBuilder().build());
      } else if (value instanceof HiveDialect) {
        builder.setHiveDialect(ObjectTypes.HiveDialect.newBuilder().build());
      } else if (value instanceof SparkDialect) {
        builder.setSparkDialect(ObjectTypes.SparkDialect.newBuilder().build());
      } else if (value instanceof TrinoDialect) {
        builder.setTrinoDialect(ObjectTypes.TrinoDialect.newBuilder().build());
      } else {
        throw new IllegalArgumentException("Unknown type" + value);
      }
      return builder;
    }

    private Dialect convertToDialect(ObjectTypes.Dialect dialect) {
      switch (dialect.getObjectTypeCase()) {
        case DREMIO_DIALECT:
          return ImmutableDremioDialect.builder().build();
        case HIVE_DIALECT:
          return ImmutableHiveDialect.builder().build();
        case SPARK_DIALECT:
          return ImmutableSparkDialect.builder().build();
        case TRINO_DIALECT:
          return ImmutableTrinoDialect.builder().build();
        case OBJECTTYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unknown type" + dialect.getObjectTypeCase());
      }
    }

    @Override
    public Byte getPayload(Contents value) {
      if (value instanceof IcebergTable) {
        return (byte) Contents.Type.ICEBERG_TABLE.ordinal();
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
        throw new IllegalArgumentException(String.format("Cannot create type from payload. Payload %d does not exist", payload));
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

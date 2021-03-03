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
package org.projectnessie.server.providers;

import java.io.IOException;
import java.util.stream.Collectors;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.DeltaLakeTable;
import org.projectnessie.model.HiveDatabase;
import org.projectnessie.model.HiveTable;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableDeltaLakeTable;
import org.projectnessie.model.ImmutableDeltaLakeTable.Builder;
import org.projectnessie.model.ImmutableHiveDatabase;
import org.projectnessie.model.ImmutableHiveTable;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.SqlView.Dialect;
import org.projectnessie.store.ObjectTypes.PContents;
import org.projectnessie.store.ObjectTypes.PDeltaLakeTable;
import org.projectnessie.store.ObjectTypes.PHiveDatabase;
import org.projectnessie.store.ObjectTypes.PHiveTable;
import org.projectnessie.store.ObjectTypes.PIcebergTable;
import org.projectnessie.store.ObjectTypes.PSqlView;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;

public class TableCommitMetaStoreWorker implements StoreWorker<Contents, CommitMeta> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Serializer<Contents> tableSerializer = new TableValueSerializer();
  private final Serializer<CommitMeta> metaSerializer = new MetadataSerializer();

  @Override
  public Serializer<Contents> getValueSerializer() {
    return tableSerializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  private static class TableValueSerializer implements Serializer<Contents> {
    @Override
    public ByteString toBytes(Contents value) {
      PContents.Builder builder = PContents.newBuilder();
      if (value instanceof IcebergTable) {
        builder.setIcebergTable(
            PIcebergTable.newBuilder().setMetadataLocation(((IcebergTable) value).getMetadataLocation()));

      } else if (value instanceof DeltaLakeTable) {

        PDeltaLakeTable.Builder table = PDeltaLakeTable.newBuilder()
            .addAllMetadataLocationHistory(((DeltaLakeTable) value).getMetadataLocationHistory())
            .addAllCheckpointLocationHistory(((DeltaLakeTable) value).getCheckpointLocationHistory());
        String lastCheckpoint = ((DeltaLakeTable) value).getLastCheckpoint();
        if (lastCheckpoint != null) {
          table.setLastCheckpoint(lastCheckpoint);
        }
        builder.setDeltaLakeTable(table);

      } else if (value instanceof HiveTable) {
        HiveTable ht = (HiveTable) value;
        builder.setHiveTable(PHiveTable.newBuilder()
            .setTable(UnsafeByteOperations.unsafeWrap(ht.getTableDefinition())).addAllPartition(
                ht.getPartitions().stream().map(UnsafeByteOperations::unsafeWrap).collect(Collectors.toList())));

      } else if (value instanceof HiveDatabase) {
        builder.setHiveDatabase(PHiveDatabase.newBuilder()
            .setDatabase(UnsafeByteOperations.unsafeWrap(((HiveDatabase) value).getDatabaseDefinition())));
      } else if (value instanceof SqlView) {
        SqlView view = (SqlView) value;
        builder.setSqlView(PSqlView.newBuilder().setDialect(view.getDialect().name()).setSqlText(view.getSqlText()));
      } else {
        throw new IllegalArgumentException("Unknown type" + value);
      }

      return builder.build().toByteString();
    }

    @Override
    public Contents fromBytes(ByteString bytes) {
      PContents contents;
      try {
        contents = PContents.parseFrom(bytes);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Failure parsing data", e);
      }
      switch (contents.getObjectTypeCase()) {
        case DELTA_LAKE_TABLE:
          Builder builder = ImmutableDeltaLakeTable.builder()
              .addAllMetadataLocationHistory(contents.getDeltaLakeTable().getMetadataLocationHistoryList())
              .addAllCheckpointLocationHistory(contents.getDeltaLakeTable().getCheckpointLocationHistoryList());
          if (contents.getDeltaLakeTable().getLastCheckpoint() != null) {
            builder.lastCheckpoint(contents.getDeltaLakeTable().getLastCheckpoint());
          }
          return builder.build();

        case HIVE_DATABASE:
          return ImmutableHiveDatabase.builder()
              .databaseDefinition(contents.getHiveDatabase().getDatabase().toByteArray()).build();

        case HIVE_TABLE:
          return ImmutableHiveTable.builder()
              .addAllPartitions(contents.getHiveTable().getPartitionList().stream().map(ByteString::toByteArray)
                  .collect(Collectors.toList()))
              .tableDefinition(contents.getHiveTable().getTable().toByteArray()).build();

        case ICEBERG_TABLE:
          return ImmutableIcebergTable.builder().metadataLocation(contents.getIcebergTable().getMetadataLocation())
              .build();

        case SQL_VIEW:
          PSqlView view = contents.getSqlView();
          return ImmutableSqlView.builder().dialect(Dialect.valueOf(view.getDialect())).sqlText(view.getSqlText())
              .build();

        case OBJECTTYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unknown type" + contents.getObjectTypeCase());

      }
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
            .commiter("unknown")
            .email("unknown")
            .hash("unknown")
            .build();
      }
    }
  }

}

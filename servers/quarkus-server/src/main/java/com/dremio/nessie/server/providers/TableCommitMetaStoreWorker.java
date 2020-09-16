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
package com.dremio.nessie.server.providers;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Singleton;

import com.dremio.nessie.jgit.ObjectTypes.PContents;
import com.dremio.nessie.jgit.ObjectTypes.PDeltaLakeTable;
import com.dremio.nessie.jgit.ObjectTypes.PHiveDatabase;
import com.dremio.nessie.jgit.ObjectTypes.PHiveTable;
import com.dremio.nessie.jgit.ObjectTypes.PIcebergTable;
import com.dremio.nessie.jgit.ObjectTypes.PSqlView;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.DeltaLakeTable;
import com.dremio.nessie.model.HiveDatabase;
import com.dremio.nessie.model.HiveTable;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableCommitMeta;
import com.dremio.nessie.model.ImmutableDeltaLakeTable;
import com.dremio.nessie.model.ImmutableHiveDatabase;
import com.dremio.nessie.model.ImmutableHiveTable;
import com.dremio.nessie.model.ImmutableIcebergTable;
import com.dremio.nessie.model.ImmutableSqlView;
import com.dremio.nessie.model.SqlView;
import com.dremio.nessie.model.SqlView.Dialect;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnsafeByteOperations;

@Singleton
public class TableCommitMetaStoreWorker implements StoreWorker<Contents, CommitMeta> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final Serializer<Contents> tableSerializer = serializer();
  private final Serializer<CommitMeta> metaSerializer = metadataSerializer();

  private Serializer<Contents> serializer() {
    //todo not handling TableMetadata at all...probably need to combine it w/ AssetKey
    return new Serializer<Contents>() {
      @Override
      public ByteString toBytes(Contents value) {
        PContents.Builder builder = PContents.newBuilder();
        if (value instanceof IcebergTable) {
          builder.setIcebergTable(PIcebergTable.newBuilder()
              .setMetadataLocation(((IcebergTable) value)
                  .getMetadataLocation()));

        } else if (value instanceof DeltaLakeTable) {
          builder.setDeltaLakeTable(PDeltaLakeTable.newBuilder()
              .setMetadataLocation(((DeltaLakeTable) value).getMetadataLocation()));

        } else if (value instanceof HiveTable) {
          HiveTable ht = (HiveTable) value;
          builder.setHiveTable(PHiveTable.newBuilder()
              .setTable(UnsafeByteOperations.unsafeWrap(ht.getTableDefinition()))
              .addAllPartition(ht.getPartitions().stream().map(UnsafeByteOperations::unsafeWrap).collect(Collectors.toList())));

        } else if (value instanceof HiveDatabase) {
          builder.setHiveDatabase(PHiveDatabase.newBuilder()
              .setDatabase(UnsafeByteOperations.unsafeWrap(((HiveDatabase) value).getDatabaseDefinition())));
        } else if (value instanceof SqlView) {
          SqlView view = (SqlView) value;
          builder.setSqlView(PSqlView.newBuilder()
              .setDialect(view.getDialect().name())
              .setSqlText(view.getSqlText()));
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
        switch(contents.getObjectTypeCase()) {
        case DELTA_LAKE_TABLE:
          return ImmutableDeltaLakeTable.builder().metadataLocation(contents.getDeltaLakeTable().getMetadataLocation()).build();

        case HIVE_DATABASE:
          return ImmutableHiveDatabase.builder().databaseDefinition(contents.getHiveDatabase().getDatabase().toByteArray()).build();

        case HIVE_TABLE:
          return ImmutableHiveTable.builder()
              .addAllPartitions(contents.getHiveTable().getPartitionList().stream().map(ByteString::toByteArray).collect(Collectors.toList()))
              .tableDefinition(contents.getHiveTable().getTable().toByteArray())
              .build();

        case ICEBERG_TABLE:
          return ImmutableIcebergTable.builder().metadataLocation(contents.getIcebergTable().getMetadataLocation()).build();

        case SQL_VIEW:
          PSqlView view = contents.getSqlView();
          return ImmutableSqlView.builder()
              .dialect(Dialect.valueOf(view.getDialect()))
              .sqlText(view.getSqlText())
              .build();

        case OBJECTTYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unknown type" + contents.getObjectTypeCase());

        }
      }
    };
  }

  private Serializer<CommitMeta> metadataSerializer() {

    return new Serializer<CommitMeta>() {
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
    };
  }

  @Override
  public Serializer<Contents> getValueSerializer() {
    return tableSerializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  @Override
  public Stream<AssetKey> getAssetKeys(Contents table) {
    throw new UnsupportedOperationException("No serialization available for AssetKey");
  }

  @Override
  public CompletableFuture<Void> deleteAsset(AssetKey key) {
    throw new UnsupportedOperationException("No serialization available for AssetKey");
  }
}

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
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.CommitMeta.Action;
import com.dremio.nessie.model.ImmutableCommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.impl.experimental.ProtoUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

@Singleton
public class TableCommitMetaStoreWorker implements StoreWorker<Table, CommitMeta> {
  private final ObjectMapper mapper;
  private final Serializer<Table> tableSerializer = serializer();
  private final Serializer<CommitMeta> metaSerializer = metadataSerializer();

  @Inject
  public TableCommitMetaStoreWorker(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  private Serializer<Table> serializer() {
    //todo not handling TableMetadata at all...probably need to combine it w/ AssetKey
    return new Serializer<Table>() {
      @Override
      public ByteString toBytes(Table value) {
        return ProtoUtil.tableToProtoc(value, null).toByteString();
      }

      @Override
      public Table fromBytes(ByteString bytes) {
        Entry<Table, String> entry = ProtoUtil.tableFromBytes(bytes.toByteArray());
        return entry.getKey();
      }
    };
  }

  private Serializer<CommitMeta> metadataSerializer() {

    return new Serializer<CommitMeta>() {
      @Override
      public ByteString toBytes(CommitMeta value) {
        try {
          return ByteString.copyFrom(mapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(String.format("Couldn't serialize commit meta %s", value), e);
        }
      }

      @Override
      public CommitMeta fromBytes(ByteString bytes) {
        try {
          return mapper.readValue(bytes.toByteArray(), CommitMeta.class);
        } catch (IOException e) {
          return ImmutableCommitMeta.builder()
                                    .action(Action.UNKNOWN)
                                    .changes(0)
                                    .comment("unknown")
                                    .commiter("unknown")
                                    .email("unknown")
                                    .ref("unknown")
                                    .build();
        }
      }
    };
  }

  @Override
  public Serializer<Table> getValueSerializer() {
    return tableSerializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return metaSerializer;
  }

  @Override
  public Stream<AssetKey> getAssetKeys(Table table) {
    throw new UnsupportedOperationException("No serialization available for AssetKey");
  }

  @Override
  public CompletableFuture<Void> deleteAsset(AssetKey key) {
    throw new UnsupportedOperationException("No serialization available for AssetKey");
  }
}

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
package com.dremio.nessie.services;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import javax.enterprise.inject.Default;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.impl.experimental.ProtoUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;

/**
 * This exists only to help HK2 deal with Generics with dependency injection.
 *
 * <p>
 *   todo Remove this and move to CDI
 * </p>
 */
@Default
public class TableCommitMetaStoreWorkerImpl implements TableCommitMetaStoreWorker {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final Serializer<Table> tableSerializer = serializer();
  private final Serializer<CommitMeta> metaSerializer = metadataSerializer();

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
          throw new RuntimeException(String.format("Couldn't parse commit meta %s", bytes.toStringUtf8()), e);
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

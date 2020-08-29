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
package com.dremio.nessie.jgit;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.eclipse.jgit.lib.Repository;

import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Table;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.google.protobuf.ByteString;

/**
 * temporary class to implement serialization of Table/CommitMeta.
 *
 * <p>
 *   will be removed/moved when refactor to VersionStore is finished.
 * </p>
 */
public class TableCommitMetaStoreWorker implements StoreWorker<Table, CommitMeta> {

  private final Repository repository;
  private final Serializer<Table> serializer;

  public TableCommitMetaStoreWorker(Repository repository) {
    this.repository = repository;
    serializer = serializer();
  }

  private Serializer<Table> serializer() {
    return new Serializer<Table>() {
      @Override
      public ByteString toBytes(Table value) {
        String id;
        try {
          id = MetadataHandler.commit(value, repository.newObjectInserter());
        } catch (IOException e) {
          //couldn't persist metadata...ignore
          id = null;
        }
        return ProtoUtil.tableToProtoc(value, id).toByteString();
      }

      @Override
      public Table fromBytes(ByteString bytes) {
        Entry<Table, String> entry = ProtoUtil.tableFromBytes(bytes.toByteArray());
        try {
          return MetadataHandler.fetch(entry, true, repository);
        } catch (IOException e) {
          //failed to get metadata
          return entry.getKey();
        }
      }
    };
  }

  @Override
  public Serializer<Table> getValueSerializer() {
    return serializer;
  }

  @Override
  public Serializer<CommitMeta> getMetadataSerializer() {
    return null;
  }

  @Override
  public Stream<AssetKey> getAssetKeys(Table table) {
    return null;
  }

  @Override
  public CompletableFuture<Void> deleteAsset(AssetKey key) {
    return null;
  }
}

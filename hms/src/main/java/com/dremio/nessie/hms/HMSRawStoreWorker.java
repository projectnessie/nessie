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
package com.dremio.nessie.hms;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.dremio.nessie.hms.HMSProto.CommitMetadata;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;

class HMSRawStoreWorker implements StoreWorker<Item, CommitMetadata> {

  @Override
  public Serializer<Item> getValueSerializer() {
    return new ItemSerializer();
  }

  @Override
  public Serializer<CommitMetadata> getMetadataSerializer() {
    return new CommitSerializer();
  }

  @Override
  public Stream<AssetKey> getAssetKeys(Item value) {
    return Stream.of();
  }

  @Override
  public CompletableFuture<Void> deleteAsset(AssetKey key) {
    return CompletableFuture.completedFuture(null);
  }
}
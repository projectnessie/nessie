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
package com.dremio.nessie.versioned;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

/**
 * A set of methods that a users of a VersionStore must implement.
 *
 * @param <VALUE> The value type saved in the VersionStore.
 * @param <COMMIT_METADATA> The commit metadata type saved in the VersionStore.
 */
public interface StoreWorker<VALUE, COMMIT_METADATA> {

  Serializer<VALUE> getValueSerializer();

  Serializer<COMMIT_METADATA> getMetadataSerializer();

  /**
   * Get a stream of AssetKeys associated with a particular value.
   *
   * @param value The value to retrieve asset keys for.
   * @return The asset keys associated with this value (if any).
   */
  Stream<AssetKey> getAssetKeys(VALUE value);

  /**
   * An idempotent deletion. Given the lack of consistency guarantees in deletion of external assets, Nessie may call
   * this method multiple times on the same AssetKey.
   */
  CompletableFuture<Void> deleteAsset(AssetKey key);

  /**
   * An asset is an external object that is mapped to a value. As part of garbage collection, assets will be deleted as
   * necessary to ensure assets are not existing if their references in Nessie are removed. This key is a unique pointer
   * to that asset that implements equality and hashcode to ensure Nessie storage can determine the unique set of asset
   * keys that must be deleted.
   *
   * <p>TODO: add some kind of mechanism for allowing Nessie to use BloomFilters with these keys.
   */
  interface AssetKey {

    @Override
    boolean equals(Object other);

    @Override
    int hashCode();


  }
}

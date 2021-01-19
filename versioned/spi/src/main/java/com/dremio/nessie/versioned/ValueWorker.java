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

import java.util.stream.Stream;

/**
 * An extended form of {@link Serializer} that is used for Values to expose secondary properties
 * associated with each value.
 *
 * @param <VALUE> The type of object stored in the TieredVersionStore
 */
public interface ValueWorker<VALUE> extends Serializer<VALUE> {

  /**
   * Get a stream of assets associated with a particular value.
   *
   * <p>Values may have zero or more assets.
   * <p>Internally, an implementation may actually track multiple types of assets under a single AssetKey heading. For
   * example, an Iceberg table's assets would include: metadata file, manifest list for each snapshot, manifests and
   * Parquet files.
   *
   * @return The asset keys associated with this value (if any).
   */
  Stream<? extends AssetKey> getAssetKeys(VALUE value);

  /**
   * Get a serializer for the AssetKey objects associated with the provided VALUE.
   *
   * @return The serializer for the AssetKey
   */
  Serializer<AssetKey> getAssetKeySerializer();

}

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

import java.nio.ByteBuffer;
import java.util.stream.Stream;

/**
 * Used to serialize & deserialize the values in the store. Provided to an implementation of VersionStore on construction.
 */
public interface Serializer<V> {

  ByteBuffer toBytes(V value);

  V fromBytes(ByteBuffer bytes);

  Stream<Asset> getAssets(V value);

  /**
   * An asset is an external object that is mapped to a value. As part of garbage collection, assets will be deleted as
   * necessary to ensure assets are not existing if their references in Nessie are removed.
   */
  interface Asset extends Comparable<Asset> {

    /**
     * An idempotent deletion. Given the lack of consistency guarantees in deletion of external assets, Nessie may call this method multiple times on equivalen assets.
     */
    void delete();

    @Override
    boolean equals(Object other);

    @Override
    int hashCode();


  }
}

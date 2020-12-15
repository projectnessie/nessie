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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.google.common.hash.PrimitiveSink;
import com.google.protobuf.ByteString;

/**
 * Assets associated with one or more values.
 *
 * <p>Assets are pointers in a reference tree. The purpose of an Asset object is to expose the items pointed to by a stored value
 */
public abstract class AssetKey<T> {

  /**
   * An idempotent deletion.
   *
   * <p>Given the lack of consistency guarantees in deletion of external assets, Nessie may call
   * this method multiple times on the same AssetKey. This future should:
   * <ul>
   * <li>Return {@code True} if the asset was deleted.
   * <li>Return {@code False} if the asset was previously deleted and no longer exists.
   * <li>Throw an exception if the asset could not be deleted.
   * </ul>
   */
  public abstract CompletableFuture<Boolean> delete();

  /**
   * Get additional assets that this asset may point to.
   *
   * <p>An asset can have child assets. While implementers could collapse this relationship, in many cases
   * it is cleaner to expose the tree shape to Nessie to simplify implementation.
   *
   * <p>Note that while Assets can be exposed by implementers as a multi-level tree, for actual use purposes,
   * the levels have no meaning and consumers of this interface treat the assets the same despite any tree
   * shape they may have exposed. (For example, this means that equals must behave correctly when comparing
   * items from different levels.)
   * @return Any children assets associated with this asset.
   */
  public Stream<AssetKey<T>> getAssetKeys() {
    return Stream.of();
  }

  /**
   * Add this AssetKey to a BloomFilter.
   *
   * @param sink The sink to populate
   */
  public abstract void addToBloomFilter(PrimitiveSink sink);

  /**
   * Expose a description of this object to be used for reporting purposes.
   *
   * @return A tuple of names associated with this object.
   */
  public abstract List<String> toReportableName();

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract int hashCode();

  public abstract static class NoOpAssetKey extends AssetKey<AssetKey<?>> {

    public static final Serializer<NoOpAssetKey> SERIALIZER = new Serializer<NoOpAssetKey>() {

      @Override
      public ByteString toBytes(NoOpAssetKey value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public NoOpAssetKey fromBytes(ByteString bytes) {
        throw new UnsupportedOperationException();
      }
    };

    private NoOpAssetKey() {
    }


  }
}

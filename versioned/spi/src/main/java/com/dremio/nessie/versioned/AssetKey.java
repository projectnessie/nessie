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
import java.util.concurrent.CompletionStage;

import com.google.protobuf.ByteString;

/**
 * Assets associated with one or more values.
 *
 * <p>Assets are pointers in a reference tree. The purpose of an Asset object is to expose the items pointed to by a stored value
 */
public abstract class AssetKey {

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
  public abstract CompletionStage<Boolean> delete();

  /**
   * Expose a description of this object to be used for reporting purposes.
   *
   * @return A tuple of names associated with this object.
   */
  public abstract List<String> toReportableName();

  // included to ensure that an implementor overrides.
  @Override
  public abstract boolean equals(Object other);


  // included to ensure that an implementor overrides.
  @Override
  public abstract int hashCode();

  public abstract static class NoOpAssetKey extends AssetKey {

    public static final Serializer<AssetKey> SERIALIZER = new Serializer<AssetKey>() {

      @Override
      public ByteString toBytes(AssetKey value) {
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

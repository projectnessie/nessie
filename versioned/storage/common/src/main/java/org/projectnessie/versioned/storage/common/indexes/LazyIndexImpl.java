/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.indexes;

import static org.projectnessie.versioned.storage.common.util.SupplyOnce.memoize;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;

final class LazyIndexImpl<V> implements StoreIndex<V> {

  private final Supplier<StoreIndex<V>> loader;
  private boolean loaded;
  private ObjId objId;
  private final StoreKey firstKey;
  private final StoreKey lastKey;

  LazyIndexImpl(Supplier<StoreIndex<V>> supplier, StoreKey firstKey, StoreKey lastKey) {
    this.firstKey = firstKey;
    this.lastKey = lastKey;
    this.loader =
        memoize(
            () -> {
              try {
                return supplier.get();
              } finally {
                loaded = true;
              }
            });
  }

  private StoreIndex<V> loaded() {
    return loader.get();
  }

  @Override
  public ObjId getObjId() {
    return objId;
  }

  @Override
  public StoreIndex<V> setObjId(ObjId objId) {
    this.objId = objId;
    return this;
  }

  @Override
  public boolean isModified() {
    if (!loaded) {
      return false;
    }
    return loaded().isModified();
  }

  @Override
  public StoreIndex<V> loadIfNecessary(Set<StoreKey> keys) {
    return loaded().loadIfNecessary(keys);
  }

  @Override
  public boolean isLoaded() {
    return loaded;
  }

  @Override
  public StoreIndex<V> asMutableIndex() {
    return loaded().asMutableIndex();
  }

  @Override
  public boolean isMutable() {
    return false;
  }

  @Override
  public List<StoreIndex<V>> divide(int parts) {
    throw new UnsupportedOperationException("Operation not supported for non-mutable indexes");
  }

  @Override
  public List<StoreIndex<V>> stripes() {
    return loaded().stripes();
  }

  @Override
  public int elementCount() {
    return loaded().elementCount();
  }

  @Override
  public int estimatedSerializedSize() {
    return loaded().estimatedSerializedSize();
  }

  @Override
  public boolean add(@Nonnull StoreIndexElement<V> element) {
    return loaded().add(element);
  }

  @Override
  public void updateAll(Function<StoreIndexElement<V>, V> updater) {
    loaded().updateAll(updater);
  }

  @Override
  public boolean remove(@Nonnull StoreKey key) {
    return loaded().remove(key);
  }

  @Override
  public boolean contains(@Nonnull StoreKey key) {
    if (!loaded && (key.equals(firstKey) || key.equals(lastKey))) {
      return true;
    }
    return loaded().contains(key);
  }

  @Override
  @Nullable
  public StoreIndexElement<V> get(@Nonnull StoreKey key) {
    return loaded().get(key);
  }

  @Override
  @Nullable
  public StoreKey first() {
    if (loaded || firstKey == null) {
      return loaded().first();
    }
    return firstKey;
  }

  @Override
  @Nullable
  public StoreKey last() {
    if (loaded || lastKey == null) {
      return loaded().last();
    }
    return lastKey;
  }

  @Override
  public List<StoreKey> asKeyList() {
    return loaded().asKeyList();
  }

  @Override
  @Nonnull
  public Iterator<StoreIndexElement<V>> iterator() {
    return loaded().iterator();
  }

  @Override
  @Nonnull
  public Iterator<StoreIndexElement<V>> iterator(
      @Nullable StoreKey begin, @Nullable StoreKey end, boolean prefetch) {
    return loaded().iterator(begin, end, prefetch);
  }

  @Override
  @Nonnull
  public ByteString serialize() {
    return loaded().serialize();
  }
}

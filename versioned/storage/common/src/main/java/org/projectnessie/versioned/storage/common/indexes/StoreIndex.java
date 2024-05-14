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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.persist.ObjId;

/**
 * General interface for all store indexes.
 *
 * <p>Store indexes are generally not thread-safe.
 *
 * @param <V> value type held in a {@link StoreIndexElement}
 */
public interface StoreIndex<V> extends Iterable<StoreIndexElement<V>> {

  boolean isModified();

  default ObjId getObjId() {
    throw new UnsupportedOperationException();
  }

  default StoreIndex<V> setObjId(ObjId objId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Load the index, if this instance represents a lazily loaded index instance.
   *
   * @return the non-lazy instance
   */
  StoreIndex<V> loadIfNecessary(Set<StoreKey> keys);

  boolean isLoaded();

  StoreIndex<V> asMutableIndex();

  boolean isMutable();

  List<StoreIndex<V>> divide(int parts);

  List<StoreIndex<V>> stripes();

  /**
   * Returns the number of elements in this index - <em>do not use this method</em> in production
   * code against lazy or striped or layered indexes, because it will trigger index load operations.
   */
  int elementCount();

  /**
   * Get the <em>estimated</em> serialized size of this structure. The returned value is likely
   * higher than the real serialized size, as produced by {@link #serialize()}, but the returned
   * value must never be smaller than the real required serialized size.
   */
  int estimatedSerializedSize();

  /**
   * Adds the given key element or updates the {@link ObjId} if the {@link StoreKey} already
   * existed.
   *
   * @return {@code true} if the {@link StoreKey} didn't exist or {@code false} if the key was
   *     already present and the operation only updated the {@link ObjId}.
   */
  boolean add(@Nonnull StoreIndexElement<V> element);

  /**
   * Functionality to update all element values in this data structure.
   *
   * @param updater called to update a value. If {@code updater} returns the same object as {@link
   *     StoreIndexElement#content()}, no change happens. If {@code updater} returns {@code null},
   *     the element will be removed. If {@code updater} returns another value, the value of the
   *     element will be updated to that value.
   */
  void updateAll(Function<StoreIndexElement<V>, V> updater);

  /**
   * Removes the index element for the given key.
   *
   * @return {@code true} if the {@link StoreKey} did exist and was removed, {@code false}
   *     otherwise.
   */
  boolean remove(@Nonnull StoreKey key);

  boolean contains(@Nonnull StoreKey key);

  @Nullable
  StoreIndexElement<V> get(@Nonnull StoreKey key);

  @Nullable
  StoreKey first();

  @Nullable
  StoreKey last();

  /** A read-only view to the {@link StoreKey}s in this data structure. */
  List<StoreKey> asKeyList();

  /** Convenience for {@link #iterator(StoreKey, StoreKey, boolean) iterator(null, null, false)}. */
  @Override
  default Iterator<StoreIndexElement<V>> iterator() {
    return iterator(null, null, false);
  }

  /**
   * Iterate over the elements in this index, with optional begin/end or prefix restrictions.
   *
   * <p><em>Prefix queries: </em> {@code begin} and {@code end} must be equal and not {@code null},
   * only elements that start with the given key value will be returned.
   *
   * <p><em>Start at queries: </em>Start at {@code begin} (inclusive)
   *
   * <p><em>End at queries: </em>End at {@code end} (inclusive if exact match) restrictions
   *
   * <p><em>Range queries: </em>{@code begin} (inclusive) and {@code end} (inclusive if exact match)
   * restrictions
   *
   * @param prefetch Enables eager prefetch of all potentially required indexes. Set to {@code
   *     false}, when using result paging.
   */
  @Nonnull
  Iterator<StoreIndexElement<V>> iterator(
      @Nullable StoreKey begin, @Nullable StoreKey end, boolean prefetch);

  @Nonnull
  ByteString serialize();
}

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

import static java.util.Collections.singletonList;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

/**
 * Combines two {@link StoreIndex store indexes}, where one index serves as the "reference" and the
 * other containing "updates".
 *
 * <p>A layered index contains all keys from both indexes. The value of a key that is present in
 * both indexes will be provided from the "updates" index.
 */
final class LayeredIndexImpl<V> implements StoreIndex<V> {

  private final StoreIndex<V> reference;
  private final StoreIndex<V> updates;

  LayeredIndexImpl(StoreIndex<V> reference, StoreIndex<V> updates) {
    this.reference = reference;
    this.updates = updates;
  }

  @Override
  public boolean isModified() {
    return updates.isModified() || reference.isModified();
  }

  @Override
  public StoreIndex<V> loadIfNecessary(Set<StoreKey> keys) {
    reference.loadIfNecessary(keys);
    updates.loadIfNecessary(keys);
    return this;
  }

  @Override
  public boolean isLoaded() {
    return reference.isLoaded() && updates.isLoaded();
  }

  @Override
  public StoreIndex<V> asMutableIndex() {
    throw new UnsupportedOperationException("Layered instance cannot be resolved");
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
    return singletonList(this);
  }

  @Override
  @SuppressWarnings("UnusedVariable")
  public int elementCount() {
    int cnt = 0;
    for (StoreIndexElement<V> ignore : this) {
      cnt++;
    }
    return cnt;
  }

  @Override
  public List<StoreKey> asKeyList() {
    List<StoreKey> keys = new ArrayList<>();
    for (StoreIndexElement<V> el : this) {
      keys.add(el.key());
    }
    return keys;
  }

  @Override
  public int estimatedSerializedSize() {
    return reference.estimatedSerializedSize() + updates.estimatedSerializedSize();
  }

  @Override
  public boolean contains(@Nonnull StoreKey key) {
    return updates.contains(key) || reference.contains(key);
  }

  @Nullable
  @Override
  public StoreIndexElement<V> get(@Nonnull StoreKey key) {
    StoreIndexElement<V> v = updates.get(key);
    return v != null ? v : reference.get(key);
  }

  @Nullable
  @Override
  public StoreKey first() {
    StoreKey f = reference.first();
    StoreKey i = updates.first();
    if (f == null) {
      return i;
    }
    if (i == null) {
      return f;
    }
    return f.compareTo(i) < 0 ? f : i;
  }

  @Nullable
  @Override
  public StoreKey last() {
    StoreKey f = reference.last();
    StoreKey i = updates.last();
    if (f == null) {
      return i;
    }
    if (i == null) {
      return f;
    }
    return f.compareTo(i) > 0 ? f : i;
  }

  @Nonnull
  @Override
  public Iterator<StoreIndexElement<V>> iterator(
      @Nullable StoreKey begin, @Nullable StoreKey end, boolean prefetch) {
    return new AbstractIterator<>() {
      final Iterator<StoreIndexElement<V>> fullIter = reference.iterator(begin, end, prefetch);
      final Iterator<StoreIndexElement<V>> incrementalIter = updates.iterator(begin, end, prefetch);

      StoreIndexElement<V> fullElement;
      StoreIndexElement<V> incrementalElement;

      @Override
      protected StoreIndexElement<V> computeNext() {
        if (fullElement == null) {
          if (fullIter.hasNext()) {
            fullElement = fullIter.next();
          }
        }
        if (incrementalElement == null) {
          if (incrementalIter.hasNext()) {
            incrementalElement = incrementalIter.next();
          }
        }

        int cmp;
        if (incrementalElement == null) {
          if (fullElement == null) {
            return endOfData();
          }

          cmp = -1;
        } else if (fullElement == null) {
          cmp = 1;
        } else {
          cmp = fullElement.key().compareTo(incrementalElement.key());
        }

        if (cmp == 0) {
          fullElement = null;
          return returnIncremental();
        }
        if (cmp < 0) {
          return returnFull();
        }
        return returnIncremental();
      }

      private StoreIndexElement<V> returnFull() {
        StoreIndexElement<V> e = fullElement;
        fullElement = null;
        return e;
      }

      private StoreIndexElement<V> returnIncremental() {
        StoreIndexElement<V> e = incrementalElement;
        incrementalElement = null;
        return e;
      }
    };
  }

  @Nonnull
  @Override
  public ByteString serialize() {
    throw unsupported();
  }

  @Override
  public boolean add(@Nonnull StoreIndexElement<V> element) {
    throw unsupported();
  }

  @Override
  public void updateAll(Function<StoreIndexElement<V>, V> updater) {
    throw unsupported();
  }

  @Override
  public boolean remove(@Nonnull StoreKey key) {
    throw unsupported();
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Layered indexes do not support updates");
  }
}

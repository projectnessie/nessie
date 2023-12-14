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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static java.util.Arrays.binarySearch;

import com.google.common.collect.AbstractIterator;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

final class StripedIndexImpl<V> implements StoreIndex<V> {

  private final StoreIndex<V>[] stripes;
  private final StoreKey[] firstLastKeys;
  private final IndexLoader<V> indexLoader;

  StripedIndexImpl(
      @Nonnull StoreIndex<V>[] stripes,
      @Nonnull StoreKey[] firstLastKeys,
      IndexLoader<V> indexLoader) {
    checkArgument(stripes.length > 1);
    checkArgument(
        stripes.length * 2 == firstLastKeys.length,
        "Number of stripes (%s) must match number of first-last-keys (%s)",
        stripes.length,
        firstLastKeys.length);
    for (StoreKey firstLastKey : firstLastKeys) {
      checkArgument(firstLastKey != null, "firstLastKey must not contain any null element");
    }
    this.stripes = stripes;
    this.firstLastKeys = firstLastKeys;
    this.indexLoader = indexLoader;
  }

  @Override
  public boolean isModified() {
    for (StoreIndex<V> stripe : stripes) {
      if (stripe.isModified()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public StoreIndex<V> loadIfNecessary(Set<StoreKey> keys) {
    StoreIndex<V>[] stripes = this.stripes;
    @SuppressWarnings("unchecked")
    StoreIndex<V>[] indexesToLoad = new StoreIndex[stripes.length];

    int cnt = 0;
    for (StoreKey key : keys) {
      int idx = stripeForExistingKey(key);
      if (idx == -1) {
        continue;
      }
      StoreIndex<V> index = stripes[idx];
      if (!index.isLoaded()) {
        indexesToLoad[idx] = index;
        cnt++;
      }
    }

    if (cnt > 0) {
      loadStripes(indexesToLoad);
    }

    return this;
  }

  private void loadStripes(int firstIndex, int lastIndex) {
    StoreIndex<V>[] stripes = this.stripes;
    @SuppressWarnings("unchecked")
    StoreIndex<V>[] indexesToLoad = new StoreIndex[stripes.length];

    int cnt = 0;
    for (int idx = firstIndex; idx <= lastIndex; idx++) {
      StoreIndex<V> index = stripes[idx];
      if (!index.isLoaded()) {
        indexesToLoad[idx] = index;
        cnt++;
      }
    }

    if (cnt > 0) {
      loadStripes(indexesToLoad);
    }
  }

  private void loadStripes(StoreIndex<V>[] indexesToLoad) {
    StoreIndex<V>[] stripes = this.stripes;

    StoreIndex<V>[] loadedIndexes = indexLoader.loadIndexes(indexesToLoad);
    for (int i = 0; i < loadedIndexes.length; i++) {
      StoreIndex<V> loaded = loadedIndexes[i];
      if (loaded != null) {
        stripes[i] = loaded;
      }
    }
  }

  @Override
  public boolean isLoaded() {
    StoreIndex<V>[] stripes = this.stripes;
    for (StoreIndex<V> stripe : stripes) {
      if (stripe.isLoaded()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public StoreIndex<V> asMutableIndex() {
    return this;
  }

  @Override
  public boolean isMutable() {
    return true;
  }

  @Override
  public List<StoreIndex<V>> divide(int parts) {
    throw new UnsupportedOperationException("Striped indexes cannot be further divided");
  }

  @Override
  public List<StoreIndex<V>> stripes() {
    return asList(stripes);
  }

  @Override
  public int elementCount() {
    int sum = 0;
    StoreIndex<V>[] stripes = this.stripes;
    for (StoreIndex<V> stripe : stripes) {
      sum += stripe.elementCount();
    }
    return sum;
  }

  @Override
  public int estimatedSerializedSize() {
    int sum = 0;
    StoreIndex<V>[] stripes = this.stripes;
    for (StoreIndex<V> stripe : stripes) {
      sum += stripe.estimatedSerializedSize();
    }
    return sum;
  }

  @Override
  public boolean contains(@Nonnull StoreKey key) {
    int i = stripeForExistingKey(key);
    if (i == -1) {
      return false;
    }
    return stripes[i].contains(key);
  }

  @Nullable
  @Override
  public StoreIndexElement<V> get(@Nonnull StoreKey key) {
    int i = stripeForExistingKey(key);
    if (i == -1) {
      return null;
    }
    return stripes[i].get(key);
  }

  @Nullable
  @Override
  public StoreKey first() {
    return stripes[0].first();
  }

  @Nullable
  @Override
  public StoreKey last() {
    StoreIndex<V>[] s = stripes;
    return s[s.length - 1].last();
  }

  @Override
  public List<StoreKey> asKeyList() {
    List<StoreKey> r = new ArrayList<>(elementCount());
    for (StoreIndexElement<V> el : this) {
      r.add(el.key());
    }
    return r;
  }

  @Nonnull
  @Override
  public Iterator<StoreIndexElement<V>> iterator(
      @Nullable StoreKey begin, @Nullable StoreKey end, boolean prefetch) {
    StoreIndex<V>[] s = stripes;

    boolean prefix = begin != null && begin.equals(end);
    int start = begin == null ? 0 : indexForKey(begin);
    int stop = prefix || end == null ? s.length - 1 : indexForKey(end);

    if (prefetch) {
      loadStripes(start, stop);
    }

    Predicate<StoreKey> endCheck =
        prefix ? k -> !k.startsWith(begin) : (end != null ? k -> end.compareTo(k) < 0 : k -> false);

    return new AbstractIterator<>() {
      int stripe = start;
      Iterator<StoreIndexElement<V>> current = s[start].iterator(begin, null, prefetch);

      @Override
      protected StoreIndexElement<V> computeNext() {

        while (true) {
          boolean has = current.hasNext();
          if (has) {
            StoreIndexElement<V> v = current.next();
            if (endCheck.test(v.key())) {
              return endOfData();
            }
            return v;
          }

          stripe++;
          if (stripe > stop) {
            return endOfData();
          }
          current = s[stripe].iterator();
        }
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
    return mutableStripe(element.key()).add(element);
  }

  @Override
  public boolean remove(@Nonnull StoreKey key) {
    return mutableStripe(key).remove(key);
  }

  private StoreIndex<V> mutableStripe(StoreKey key) {
    int i = indexForKey(key);
    StoreIndex<V> stripe = stripes[i];
    if (!stripe.isMutable()) {
      stripes[i] = stripe = stripe.asMutableIndex();
    }
    return stripe;
  }

  @Override
  public void updateAll(Function<StoreIndexElement<V>, V> updater) {
    throw unsupported();
  }

  private int stripeForExistingKey(StoreKey key) {
    StoreKey[] firstLast = firstLastKeys;
    int i = binarySearch(firstLast, key);
    if (i < 0) {
      i = -i - 1;
      if ((i & 1) == 0) {
        return -1;
      }
    }
    if (i == firstLast.length) {
      return -1;
    }
    i /= 2;
    return i;
  }

  private int indexForKey(StoreKey key) {
    StoreKey[] firstLast = firstLastKeys;
    int i = binarySearch(firstLast, key);
    if (i < 0) {
      i = -i - 1;
    }
    return Math.min(i / 2, (firstLast.length / 2) - 1);
  }

  private static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException("Striped indexes do not support this operation");
  }
}

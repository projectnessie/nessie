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
import static org.projectnessie.versioned.storage.common.indexes.IndexLoader.notLoading;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;

/** Factory methods for store indexes. */
public final class StoreIndexes {
  private StoreIndexes() {}

  public static <V> StoreIndex<V> emptyImmutableIndex(ElementSerializer<V> serializer) {
    return new ImmutableEmptyIndexImpl<>(serializer);
  }

  public static <V> StoreIndex<V> newStoreIndex(ElementSerializer<V> serializer) {
    return new StoreIndexImpl<>(serializer);
  }

  public static <V> StoreIndex<V> deserializeStoreIndex(
      ByteString serialized, ElementSerializer<V> ser) {
    return StoreIndexImpl.deserializeStoreIndex(serialized.asReadOnlyByteBuffer(), ser);
  }

  /**
   * Returns a {@link StoreIndex} that calls the supplier upon the first use, useful to load an
   * index only when it is needed.
   *
   * <p>Used to load a commit {@link CommitObj#referenceIndex()} only when it is needed.
   */
  public static <V> StoreIndex<V> lazyStoreIndex(
      Supplier<StoreIndex<V>> supplier, StoreKey firstKey, StoreKey lastKey) {
    return new LazyIndexImpl<>(supplier, firstKey, lastKey);
  }

  public static <V> StoreIndex<V> lazyStoreIndex(Supplier<StoreIndex<V>> supplier) {
    return new LazyIndexImpl<>(supplier, null, null);
  }

  /**
   * Combined view of two indexes, values of the {@code updates} index take precedence.
   *
   * <p>Used to construct a combined view to a commit {@link CommitObj#referenceIndex()} plus {@link
   * CommitObj#incrementalIndex()}.
   */
  public static <V> StoreIndex<V> layeredIndex(StoreIndex<V> reference, StoreIndex<V> updates) {
    return new LayeredIndexImpl<>(reference, updates);
  }

  /**
   * Produces a new, striped index from the given segments.
   *
   * <p>Used to produce a commit {@link CommitObj#referenceIndex()} that is going to be persisted in
   * multiple database rows.
   */
  public static <V> StoreIndex<V> indexFromStripes(List<StoreIndex<V>> stripes) {
    @SuppressWarnings("unchecked")
    StoreIndex<V>[] stripesArr = stripes.toArray(new StoreIndex[0]);
    StoreKey[] firstLastKeys = new StoreKey[stripes.size() * 2];
    for (int i = 0; i < stripes.size(); i++) {
      StoreIndex<V> stripe = stripes.get(i);
      StoreKey first = stripe.first();
      StoreKey last = stripe.last();
      checkArgument(first != null && last != null, "Stipe #%s must not be empty, but is empty", i);
      firstLastKeys[i * 2] = first;
      firstLastKeys[i * 2 + 1] = last;
    }

    return new StripedIndexImpl<>(stripesArr, firstLastKeys, notLoading());
  }

  /**
   * Instantiates a striped index using the given stripes. The order of the stripes must represent
   * the natural order of the keys, which means that all keys in any stripe must be smaller than the
   * keys of any following stripe.
   *
   * <p>Used to represent a loaded commit {@link CommitObj#referenceIndex()} that is loaded from
   * multiple database rows.
   *
   * @param stripes the nested indexes, must have at least two
   * @param firstLastKeys the first+last keys of the {@code stripes}
   * @param indexLoader the bulk-loading capable lazy index loader
   */
  public static <V> StoreIndex<V> indexFromSplits(
      @Nonnull List<StoreIndex<V>> stripes,
      @Nonnull List<StoreKey> firstLastKeys,
      IndexLoader<V> indexLoader) {
    @SuppressWarnings("unchecked")
    StoreIndex<V>[] stripesArr = stripes.toArray(new StoreIndex[0]);
    StoreKey[] firstLastKeysArr = firstLastKeys.toArray(new StoreKey[0]);
    return new StripedIndexImpl<>(stripesArr, firstLastKeysArr, indexLoader);
  }
}
